from __future__ import annotations

import json
import time
from typing import Callable

from mm_recorder.exchanges.types import BookSnapshot, DepthDiff, Trade
from mm_recorder.recorder_context import RecorderContext
from mm_recorder.recorder_settings import (
    DECIMALS,
    DEPTH_LEVELS,
    HEARTBEAT_SEC,
    MAX_BUFFER_WARN,
    SNAPSHOT_LIMIT,
    SYNC_WARN_AFTER_SEC,
    WS_NO_DATA_WARN_S,
    WS_PING_INTERVAL_S,
    WS_PING_TIMEOUT_S,
    INSECURE_TLS,
)
from mm_recorder.recorder_types import RecorderPhase
from mm_recorder.snapshot import write_snapshot_csv, write_snapshot_json


class RecorderEmitter:
    def __init__(self, ctx: RecorderContext) -> None:
        self.ctx = ctx

    def _next_recv_seq(self) -> int:
        self.ctx.state.recv_seq += 1
        return self.ctx.state.recv_seq

    def _next_event_id(self) -> int:
        self.ctx.state.event_id += 1
        return self.ctx.state.event_id

    def emit_event(self, ev_type: str, details: dict | str) -> int:
        ctx = self.ctx
        if ctx.ev_f.closed:
            return -1

        eid = self._next_event_id()
        ts_recv_ms = int(time.time() * 1000)
        ts_recv_seq = self._next_recv_seq()
        details_s = json.dumps(details, ensure_ascii=False) if isinstance(details, dict) else str(details)
        ctx.ev_w.writerow([eid, ts_recv_ms, ts_recv_seq, ctx.run_id, ev_type, ctx.state.epoch_id, details_s])
        ctx.ev_f.flush()
        return eid

    def set_phase(self, new_phase: RecorderPhase, reason: str | None = None) -> None:
        ctx = self.ctx
        if ctx.state.phase == new_phase:
            return
        prev = ctx.state.phase
        ctx.state.phase = new_phase
        details = {"from": prev.value, "to": new_phase.value}
        if reason:
            details["reason"] = reason
        self.emit_event("state_change", details)

    def write_gap(self, event: str, details: str) -> None:
        ctx = self.ctx
        ts_recv_ms = int(time.time() * 1000)
        ts_recv_seq = self._next_recv_seq()
        ctx.gap_w.writerow([ts_recv_ms, ts_recv_seq, ctx.run_id, ctx.state.epoch_id, event, details])
        ctx.gap_f.flush()

    def safe_close(self, obj, label: str) -> None:
        if obj is None:
            return
        try:
            obj.close()
        except Exception:
            self.ctx.log.exception("Failed to close %s", label)


class RecorderHeartbeat:
    def __init__(self, ctx: RecorderContext, emitter: RecorderEmitter, window_now_fn: Callable[[], object]) -> None:
        self.ctx = ctx
        self.emitter = emitter
        self.window_now_fn = window_now_fn
        self.proc_t0 = time.time()
        self.stream = None

    def attach_stream(self, stream) -> None:
        self.stream = stream

    def heartbeat(self, force: bool = False) -> None:
        ctx = self.ctx
        now_s = time.time()
        if (not ctx.state.window_end_emitted) and self.window_now_fn() >= ctx.window_end:
            ctx.state.window_end_emitted = True
            self.emitter.emit_event("window_end", {"end": ctx.window_end.isoformat()})
            try:
                if self.stream:
                    self.stream.close()
            except Exception:
                ctx.log.exception("Failed to close stream on window end (heartbeat)")
            return
        if (not force) and (now_s - ctx.state.last_hb < HEARTBEAT_SEC):
            return
        ctx.state.last_hb = now_s
        uptime = now_s - self.proc_t0

        if ctx.state.ws_open_count > 0 and ctx.state.last_ws_msg_time is not None:
            idle_s = now_s - ctx.state.last_ws_msg_time
            if idle_s >= WS_NO_DATA_WARN_S and (now_s - ctx.state.last_no_data_warn) >= WS_NO_DATA_WARN_S:
                ctx.state.last_no_data_warn = now_s
                self.emitter.emit_event("ws_no_data", {"idle_s": float(idle_s)})
                ctx.log.warning("No WS data for %.1fs (phase=%s)", idle_s, ctx.state.phase.value)

        ctx.log.info(
            "HEARTBEAT uptime=%.0fs synced=%s snapshot=%s lastUpdateId=%s "
            "depth_msgs=%d trade_msgs=%d ob_rows=%d tr_rows=%d buffer=%d "
            "last_depth_E=%s last_trade_E=%s epoch_id=%d",
            uptime,
            ctx.engine.depth_synced,
            ctx.engine.snapshot_loaded,
            ctx.engine.lob.last_update_id,
            ctx.state.depth_msg_count,
            ctx.state.trade_msg_count,
            ctx.state.ob_rows_written,
            ctx.state.tr_rows_written,
            len(ctx.engine.buffer),
            ctx.state.last_depth_event_ms,
            ctx.state.last_trade_event_ms,
            ctx.state.epoch_id,
        )

    def warn_not_synced(self) -> None:
        ctx = self.ctx
        if ctx.engine.depth_synced:
            return

        if len(ctx.engine.buffer) > MAX_BUFFER_WARN:
            ctx.log.warning("Depth buffer large: %d events (not synced). lastUpdateId=%s",
                            len(ctx.engine.buffer), ctx.engine.lob.last_update_id)

        now_s = time.time()
        if (not ctx.state.window_end_emitted) and self.window_now_fn() >= ctx.window_end:
            ctx.state.window_end_emitted = True
            self.emitter.emit_event("window_end", {"end": ctx.window_end.isoformat()})
            try:
                if self.stream:
                    self.stream.close()
            except Exception:
                ctx.log.exception("Failed to close stream on window end (heartbeat)")
            return
        if (now_s - ctx.state.sync_t0) > SYNC_WARN_AFTER_SEC and (now_s - ctx.state.last_sync_warn) > SYNC_WARN_AFTER_SEC:
            ctx.state.last_sync_warn = now_s
            ctx.log.warning("Still not synced after %.0fs (buffer=%d)", now_s - ctx.state.sync_t0, len(ctx.engine.buffer))


class RecorderSnapshotter:
    def __init__(self, ctx: RecorderContext, emitter: RecorderEmitter, heartbeat: RecorderHeartbeat) -> None:
        self.ctx = ctx
        self.emitter = emitter
        self.heartbeat = heartbeat

    def fetch_snapshot(self, tag: str) -> None:
        ctx = self.ctx
        eid = self.emitter.emit_event("snapshot_request", {"tag": tag, "limit": SNAPSHOT_LIMIT})
        lob, path, last_uid, raw_snapshot = ctx.record_rest_snapshot_fn(
            client=ctx.rest_client,
            symbol=ctx.symbol,
            day_dir=ctx.day_dir,
            snapshots_dir=ctx.snapshots_dir,
            limit=SNAPSHOT_LIMIT,
            run_id=ctx.run_id,
            event_id=eid,
            tag=tag,
            decimals=DECIMALS,
        )
        raw_path = ctx.snapshots_dir / f"snapshot_{eid:06d}_{tag}.json"
        write_snapshot_json(path=raw_path, payload=raw_snapshot)

        ctx.engine.adopt_snapshot(lob)
        ctx.state.sync_t0 = time.time()
        ctx.state.last_sync_warn = time.time()

        self.emitter.emit_event(
            "snapshot_loaded",
            {"tag": tag, "lastUpdateId": last_uid, "path": str(path), "raw_path": str(raw_path)},
        )
        ctx.log.info("Snapshot %s loaded lastUpdateId=%s (%s)", tag, last_uid, path)

    def resync(self, reason: str) -> None:
        ctx = self.ctx
        ctx.state.resync_count += 1
        ctx.state.epoch_id += 1
        tag = f"resync_{ctx.state.resync_count:06d}"

        self.emitter.set_phase(RecorderPhase.RESYNCING, reason)
        ctx.log.warning("Resync triggered: %s", reason)
        self.emitter.write_gap("resync_start", reason)
        self.emitter.emit_event("resync_start", {"reason": reason, "tag": tag})

        if "checksum_mismatch" in reason and hasattr(ctx.engine, "last_checksum_payload"):
            payload = getattr(ctx.engine, "last_checksum_payload", None)
            if payload:
                debug_dir = ctx.day_dir / "debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                path = debug_dir / f"checksum_payload_{tag}.txt"
                path.write_text(payload)
                self.emitter.emit_event("checksum_payload_saved", {"tag": tag, "path": str(path)})

        ctx.engine.reset_for_resync()

        if ctx.adapter.sync_mode == "checksum":
            ctx.state.needs_snapshot = True
            ctx.state.pending_snapshot_tag = tag
            try:
                reconnect = getattr(self.heartbeat.stream, "disconnect", None) or getattr(self.heartbeat.stream, "close", None)
                if reconnect is not None:
                    reconnect()
            except Exception:
                ctx.log.exception("Failed to close stream for checksum resync")
            return

        try:
            self.fetch_snapshot(tag)
        except Exception as e:
            ctx.log.exception("Resync snapshot failed; closing WS")
            self.emitter.write_gap("fatal", f"{tag}_snapshot_failed: {e}")
            self.emitter.emit_event("fatal", {"reason": "resync_snapshot_failed", "tag": tag, "error": str(e)})
            if self.heartbeat.stream:
                self.heartbeat.stream.close()
            return

        self.emitter.write_gap("resync_done", f"tag={tag} lastUpdateId={ctx.engine.lob.last_update_id}")
        self.emitter.emit_event("resync_done", {"tag": tag, "lastUpdateId": ctx.engine.lob.last_update_id})

    def handle_snapshot(self, snapshot: BookSnapshot, tag: str) -> None:
        ctx = self.ctx
        self.emitter.set_phase(RecorderPhase.SYNCING, "snapshot_loaded")
        details = {"tag": tag, "lastUpdateId": 0}
        if snapshot.checksum is not None:
            details["checksum"] = int(snapshot.checksum)
        eid = self.emitter.emit_event("snapshot_loaded", details)
        path = ctx.snapshots_dir / f"snapshot_{eid:06d}_{tag}.csv"
        raw_path = ctx.snapshots_dir / f"snapshot_{eid:06d}_{tag}.json"
        write_snapshot_csv(
            path=path,
            run_id=ctx.run_id,
            event_id=eid,
            bids=snapshot.bids,
            asks=snapshot.asks,
            last_update_id=0,
            checksum=(int(snapshot.checksum) if snapshot.checksum is not None else None),
            decimals=DECIMALS,
        )
        if snapshot.raw is not None:
            write_snapshot_json(path=raw_path, payload=snapshot.raw)
            self.emitter.emit_event("snapshot_raw_saved", {"path": str(raw_path), "tag": tag})
        ctx.engine.adopt_snapshot(snapshot)
        ctx.state.sync_t0 = time.time()
        ctx.state.last_sync_warn = time.time()
        if tag != "initial":
            self.emitter.write_gap("resync_done", f"tag={tag} lastUpdateId=0")
            self.emitter.emit_event("resync_done", {"tag": tag, "lastUpdateId": 0})


class RecorderDepthHandler:
    def __init__(self, ctx: RecorderContext, emitter: RecorderEmitter, heartbeat: RecorderHeartbeat, snapshotter: RecorderSnapshotter) -> None:
        self.ctx = ctx
        self.emitter = emitter
        self.heartbeat = heartbeat
        self.snapshotter = snapshotter

    def write_topn(self, event_time_ms: int, recv_ms: int, recv_seq: int) -> None:
        ctx = self.ctx
        bids, asks = ctx.engine.lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

        row = [event_time_ms, recv_ms, recv_seq, ctx.run_id, ctx.state.epoch_id]
        for i in range(DEPTH_LEVELS):
            bp, bq = bids[i]
            ap, aq = asks[i]
            row += [
                f"{bp:.{DECIMALS}f}",
                f"{bq:.{DECIMALS}f}",
                f"{ap:.{DECIMALS}f}",
                f"{aq:.{DECIMALS}f}",
            ]

        ctx.ob_writer.write_row(row)
        ctx.state.ob_rows_written += 1

    def handle_depth(self, parsed: DepthDiff, recv_ms: int) -> None:
        ctx = self.ctx
        msg_recv_seq = self.emitter._next_recv_seq()

        ctx.state.depth_msg_count += 1
        ctx.state.last_depth_event_ms = int(parsed.event_time_ms)
        ctx.state.last_ws_msg_time = time.time()
        if not ctx.state.first_data_emitted:
            ctx.state.first_data_emitted = True
            self.emitter.emit_event("ws_first_data", {"type": "depth"})
            ctx.log.info("WS data flowing (first depth message).")

        if ctx.diff_writer is not None:
            try:
                minimal = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "E": int(parsed.event_time_ms),
                    "U": int(parsed.U),
                    "u": int(parsed.u),
                    "b": parsed.bids,
                    "a": parsed.asks,
                }
                if parsed.checksum is not None:
                    minimal["checksum"] = int(parsed.checksum)
                minimal["exchange"] = ctx.exchange
                minimal["symbol"] = ctx.symbol
                if parsed.raw is not None:
                    minimal["raw"] = parsed.raw
                ctx.diff_writer.write_line(json.dumps(minimal, ensure_ascii=False, default=str) + "\n")
            except Exception:
                ctx.log.exception("Failed writing depth diffs")
        if ctx.live_diff_writer is not None:
            try:
                minimal_live = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "E": int(parsed.event_time_ms),
                    "U": int(parsed.U),
                    "u": int(parsed.u),
                    "b": parsed.bids,
                    "a": parsed.asks,
                    "exchange": ctx.exchange,
                    "symbol": ctx.symbol,
                }
                if parsed.checksum is not None:
                    minimal_live["checksum"] = int(parsed.checksum)
                if parsed.raw is not None:
                    minimal_live["raw"] = parsed.raw
                ctx.live_diff_writer.write_line(json.dumps(minimal_live, ensure_ascii=False, default=str) + "\n")
            except Exception:
                ctx.log.exception("Failed writing live depth diffs")

        if (not ctx.state.window_end_emitted) and self.heartbeat.window_now_fn() >= ctx.window_end:
            ctx.state.window_end_emitted = True
            self.emitter.emit_event("window_end", {"end": ctx.window_end.isoformat()})
            try:
                if self.heartbeat.stream:
                    self.heartbeat.stream.close()
            except Exception:
                ctx.log.exception("Failed to close stream on window end")
            return

        try:
            if ctx.adapter.sync_mode == "checksum":
                result = ctx.engine.feed_depth_event(parsed)
            else:
                result = ctx.engine.feed_depth_event(
                    {"E": parsed.event_time_ms, "U": parsed.U, "u": parsed.u, "b": parsed.bids, "a": parsed.asks}
                )

            if result.action == "gap":
                self.snapshotter.resync(result.details)
                self.heartbeat.heartbeat()
                return

            if result.action in ("synced", "applied") and ctx.engine.depth_synced:
                self.emitter.set_phase(RecorderPhase.SYNCED, "depth_synced")
                self.write_topn(event_time_ms=int(parsed.event_time_ms), recv_ms=recv_ms, recv_seq=msg_recv_seq)

            if result.action == "buffered":
                self.heartbeat.warn_not_synced()

        except Exception:
            ctx.log.exception("Unhandled exception in on_depth")
            self.snapshotter.resync("exception_in_on_depth")

        finally:
            self.heartbeat.heartbeat()


class RecorderTradeHandler:
    def __init__(self, ctx: RecorderContext, emitter: RecorderEmitter, heartbeat: RecorderHeartbeat) -> None:
        self.ctx = ctx
        self.emitter = emitter
        self.heartbeat = heartbeat

    def handle_trade(self, parsed: Trade, recv_ms: int) -> None:
        ctx = self.ctx

        ctx.state.trade_msg_count += 1
        ctx.state.last_trade_event_ms = int(parsed.event_time_ms)
        ctx.state.last_ws_msg_time = time.time()
        if not ctx.state.first_data_emitted:
            ctx.state.first_data_emitted = True
            self.emitter.emit_event("ws_first_data", {"type": "trade"})
            ctx.log.info("WS data flowing (first trade message).")

        msg_recv_seq = self.emitter._next_recv_seq()

        try:
            side = parsed.side
            if side is None:
                side = "sell" if int(parsed.is_buyer_maker) == 1 else "buy"
            ctx.tr_writer.write_row(
                [
                    int(parsed.event_time_ms),
                    recv_ms,
                    msg_recv_seq,
                    ctx.run_id,
                    int(parsed.trade_id),
                    int(parsed.trade_time_ms),
                    f"{float(parsed.price):.{DECIMALS}f}",
                    f"{float(parsed.qty):.{DECIMALS}f}",
                    int(parsed.is_buyer_maker),
                    side or "",
                    parsed.ord_type or "",
                    ctx.exchange,
                    ctx.symbol,
                ]
            )
            ctx.state.tr_rows_written += 1
            raw_payload = None
            if parsed.raw is not None:
                raw_payload = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "event_time_ms": int(parsed.event_time_ms),
                    "trade_id": int(parsed.trade_id),
                    "price": parsed.price,
                    "qty": parsed.qty,
                    "side": side,
                    "exchange": ctx.exchange,
                    "symbol": ctx.symbol,
                    "raw": parsed.raw,
                }
                ctx.tr_raw_writer.write_line(json.dumps(raw_payload, ensure_ascii=False, default=str) + "\n")
            if ctx.live_trade_writer is not None:
                try:
                    live_payload = raw_payload if raw_payload is not None else {
                        "recv_ms": recv_ms,
                        "recv_seq": msg_recv_seq,
                        "event_time_ms": int(parsed.event_time_ms),
                        "trade_id": int(parsed.trade_id),
                        "price": parsed.price,
                        "qty": parsed.qty,
                        "side": side,
                        "exchange": ctx.exchange,
                        "symbol": ctx.symbol,
                    }
                    ctx.live_trade_writer.write_line(json.dumps(live_payload, ensure_ascii=False, default=str) + "\n")
                except Exception:
                    ctx.log.exception("Failed writing live trades")
        except Exception:
            ctx.log.exception(
                "Unhandled exception in on_trade trade_id=%s event_time_ms=%s recv_ms=%s",
                getattr(parsed, "trade_id", None),
                getattr(parsed, "event_time_ms", None),
                recv_ms,
            )
        finally:
            self.heartbeat.heartbeat()


class RecorderCallbacks:
    def __init__(self, ctx: RecorderContext, window_now_fn: Callable[[], object]) -> None:
        self.ctx = ctx
        self.emitter = RecorderEmitter(ctx)
        self.heartbeat = RecorderHeartbeat(ctx, self.emitter, window_now_fn)
        self.snapshotter = RecorderSnapshotter(ctx, self.emitter, self.heartbeat)
        self.depth_handler = RecorderDepthHandler(ctx, self.emitter, self.heartbeat, self.snapshotter)
        self.trade_handler = RecorderTradeHandler(ctx, self.emitter, self.heartbeat)

    def attach_stream(self, stream) -> None:
        self.heartbeat.attach_stream(stream)

    def emit_event(self, ev_type: str, details: dict | str) -> int:
        return self.emitter.emit_event(ev_type, details)

    def on_depth(self, data, recv_ms: int) -> None:
        try:
            parsed = self.ctx.adapter.parse_depth(data)
        except Exception:
            self.ctx.log.exception("Failed to parse depth message")
            return
        self.depth_handler.handle_depth(parsed, recv_ms)

    def on_trade(self, data: dict, recv_ms: int) -> None:
        try:
            parsed = self.ctx.adapter.parse_trade(data)
        except Exception:
            self.ctx.log.exception("Failed to parse trade message")
            return
        self.trade_handler.handle_trade(parsed, recv_ms)

    def on_message(self, data: dict, recv_ms: int) -> None:
        ctx = self.ctx
        ctx.state.last_ws_msg_time = time.time()
        if not ctx.state.first_data_emitted:
            ctx.state.first_data_emitted = True
            self.emitter.emit_event("ws_first_data", {"type": "custom"})
            ctx.log.info("WS data flowing (first custom message).")
        if isinstance(data, dict) and data.get("method") == "subscribe":
            self.emitter.emit_event(
                "ws_subscribe_ack",
                {
                    "success": data.get("success"),
                    "result": data.get("result"),
                    "error": data.get("error"),
                },
            )
            if data.get("error"):
                ctx.log.warning("WS subscribe error: %s", data.get("error"))
        elif isinstance(data, dict) and data.get("event") == "error":
            self.emitter.emit_event("ws_error_payload", {"error": data.get("msg") or data})
            ctx.log.warning("WS error payload: %s", data.get("msg") or data)
        elif isinstance(data, dict) and data.get("event") == "info":
            code = data.get("code")
            self.emitter.emit_event("ws_info", {"code": code, "msg": data.get("msg")})
            if code == 20051:
                self.emitter.emit_event("ws_info_reconnect", {"code": code, "msg": data.get("msg")})
                try:
                    if self.heartbeat.stream:
                        self.heartbeat.stream.disconnect()
                except Exception:
                    ctx.log.exception("Failed to disconnect after ws_info reconnect")
        elif isinstance(data, dict) and data.get("error"):
            self.emitter.emit_event("ws_error_payload", {"error": data.get("error")})
            ctx.log.warning("WS error payload: %s", data.get("error"))
        try:
            snapshots, diffs, trades = ctx.adapter.parse_ws_message(data)
        except Exception:
            ctx.log.exception("Failed to parse WS message")
            return
        for snap in snapshots:
            if ctx.state.needs_snapshot:
                tag = ctx.state.pending_snapshot_tag or "snapshot"
                self.snapshotter.handle_snapshot(snap, tag)
                ctx.state.needs_snapshot = False
                ctx.state.pending_snapshot_tag = None
        for diff in diffs:
            self.depth_handler.handle_depth(diff, recv_ms)
        for tr in trades:
            self.trade_handler.handle_trade(tr, recv_ms)

    def on_open(self) -> None:
        ctx = self.ctx
        ctx.state.ws_open_count += 1
        self.emitter.set_phase(RecorderPhase.SNAPSHOT, "ws_open")

        if ctx.state.ws_open_count == 1:
            ctx.state.epoch_id = 0
            self.emitter.emit_event(
                "ws_open",
                {
                    "ws_url": ctx.ws_url,
                    "ping_interval_s": WS_PING_INTERVAL_S,
                    "ping_timeout_s": WS_PING_TIMEOUT_S,
                    "insecure_tls": INSECURE_TLS,
                },
            )
            if ctx.adapter.sync_mode == "checksum":
                ctx.state.needs_snapshot = True
                ctx.state.pending_snapshot_tag = "initial"
            else:
                try:
                    self.snapshotter.fetch_snapshot("initial")
                except Exception as e:
                    ctx.log.exception("Failed initial snapshot; closing WS")
                    self.emitter.write_gap("fatal", f"initial_snapshot_failed: {e}")
                    self.emitter.emit_event("fatal", {"reason": "initial_snapshot_failed", "error": str(e)})
                    if self.heartbeat.stream:
                        self.heartbeat.stream.close()
        else:
            self.emitter.emit_event("ws_reconnect_open", {"ws_url": ctx.ws_url, "open_count": ctx.state.ws_open_count})
            if ctx.adapter.sync_mode == "checksum":
                if not ctx.state.needs_snapshot:
                    self.snapshotter.resync("ws_reconnect")
            else:
                self.snapshotter.resync("ws_reconnect")

    def on_status(self, typ: str, details: dict) -> None:
        self.emitter.emit_event(typ, details)
        self.ctx.log.info("WS status: %s %s", typ, details)
        if typ == "ws_connecting":
            self.emitter.set_phase(RecorderPhase.CONNECTING, "ws_connecting")

    def shutdown(self) -> None:
        ctx = self.ctx
        try:
            self.emitter.emit_event("run_stop", {"symbol": ctx.symbol})
        except Exception:
            ctx.log.exception("Failed to emit run_stop event")

        self.emitter.set_phase(RecorderPhase.STOPPED, "run_stop")
        self.heartbeat.heartbeat(force=True)

        self.emitter.safe_close(ctx.gap_f, f"file {getattr(ctx.gap_f, 'name', 'unknown')}")
        self.emitter.safe_close(ctx.ev_f, f"file {getattr(ctx.ev_f, 'name', 'unknown')}")
        self.emitter.safe_close(ctx.ob_writer, "orderbook_writer")
        self.emitter.safe_close(ctx.tr_writer, "trades_writer")
        self.emitter.safe_close(ctx.tr_raw_writer, "trades_raw_writer")
        self.emitter.safe_close(ctx.diff_writer, "diff_writer")
        self.emitter.safe_close(ctx.live_diff_writer, "live_writer")
        self.emitter.safe_close(ctx.live_trade_writer, "live_writer")
        ctx.log.info("Recorder stopped.")
