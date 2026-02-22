from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from mm_core.checksum.base import BookSnapshot as ChecksumBookSnapshot
from mm_core.checksum.base import DepthDiff as ChecksumDepthDiff
from mm_core.checksum.bitfinex import BitfinexSyncEngine
from mm_core.checksum.kraken import KrakenSyncEngine
from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import OrderBookSyncEngine
from mm_replay.reader import (
    ReplayDataError,
    ReplaySegment,
    build_segments,
    iter_diffs,
    load_snapshot_csv,
    read_events,
    resolve_paths,
)


class ReplayIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplayConfig:
    day_dir: Path
    exchange: Optional[str] = None
    top_n: int = 20
    on_error: str = "strict"  # "strict" | "best-effort"
    speed: float = 0.0
    time_base: str = "recv"  # "recv" | "event"
    validate_only: bool = False
    bitfinex_price_precision: Optional[int] = None


@dataclass
class ReplayStats:
    exchange: str
    day_dir: str
    segments_total: int = 0
    segments_replayed: int = 0
    frames_emitted: int = 0
    diffs_applied: int = 0
    gaps: int = 0
    discontinuities: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "day_dir": self.day_dir,
            "segments_total": self.segments_total,
            "segments_replayed": self.segments_replayed,
            "frames_emitted": self.frames_emitted,
            "diffs_applied": self.diffs_applied,
            "gaps": self.gaps,
            "discontinuities": self.discontinuities,
        }


def _env_bitfinex_price_precision() -> Optional[int]:
    raw = os.getenv("BITFINEX_PRICE_PRECISION")
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _make_engine(exchange: str, depth: Optional[int], bitfinex_price_precision: Optional[int]):
    ex = exchange.lower()
    if ex == "binance":
        return OrderBookSyncEngine()
    if ex == "kraken":
        return KrakenSyncEngine(depth=depth or 20)
    if ex == "bitfinex":
        return BitfinexSyncEngine(depth=25, price_precision=bitfinex_price_precision)
    raise ReplayDataError(f"Unsupported exchange for replay: {exchange}")


def _adopt_snapshot(engine, exchange: str, seg: ReplaySegment) -> None:
    snap = load_snapshot_csv(seg.snapshot_path)
    ex = exchange.lower()
    if ex == "binance":
        lob = LocalOrderBook()
        lob.load_snapshot(bids=snap.bids, asks=snap.asks, last_update_id=snap.last_update_id)
        engine.adopt_snapshot(lob)
        return
    checksum = seg.checksum if seg.checksum is not None else snap.checksum
    engine.adopt_snapshot(
        ChecksumBookSnapshot(event_time_ms=0, bids=snap.bids, asks=snap.asks, checksum=checksum)
    )


def _feed_diff(engine, exchange: str, payload: dict[str, Any]):
    ex = exchange.lower()
    if ex == "binance":
        return engine.feed_depth_event(payload)
    diff = ChecksumDepthDiff(
        event_time_ms=int(payload.get("E", 0)),
        U=int(payload.get("U", 0)),
        u=int(payload.get("u", 0)),
        bids=payload.get("b", []),
        asks=payload.get("a", []),
        checksum=(int(payload["checksum"]) if payload.get("checksum") is not None else None),
        raw=payload.get("raw"),
    )
    return engine.feed_depth_event(diff)


def _frame_from_engine(engine, exchange: str, symbol: str, seg: ReplaySegment, payload: dict[str, Any], top_n: int) -> dict[str, Any]:
    bids, asks = engine.lob.top_n(top_n)
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    return {
        "type": "book",
        "exchange": exchange,
        "symbol": symbol,
        "segment_index": seg.index,
        "segment_tag": seg.tag,
        "epoch_id": seg.epoch_id,
        "recv_seq": int(payload.get("recv_seq", 0)),
        "recv_ms": int(payload.get("recv_ms", 0)),
        "event_time_ms": int(payload.get("E", 0)),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bids": [[float(p), float(q)] for p, q in bids],
        "asks": [[float(p), float(q)] for p, q in asks],
    }


def _maybe_sleep(frame: dict[str, Any], speed: float, time_base: str, last_ts_ms: Optional[int]) -> Optional[int]:
    if speed <= 0:
        return None
    key = "recv_ms" if time_base == "recv" else "event_time_ms"
    ts_ms = int(frame.get(key) or 0)
    if last_ts_ms is None or ts_ms <= 0 or last_ts_ms <= 0:
        return ts_ms
    delta_ms = ts_ms - last_ts_ms
    if delta_ms > 0:
        time.sleep(delta_ms / 1000.0 / speed)
    return ts_ms


def replay_orderbook_day(config: ReplayConfig, emit: Optional[Callable[[dict[str, Any]], None]] = None) -> ReplayStats:
    day_dir = Path(config.day_dir)
    paths = resolve_paths(day_dir, exchange=config.exchange)
    events = read_events(paths.events_path)
    segments = build_segments(day_dir, events)
    if not segments:
        raise ReplayDataError(f"No snapshot_loaded segments found in {paths.events_path}")

    # Use symbol directory name from path for stable output metadata.
    try:
        symbol = day_dir.parent.name
    except Exception:
        symbol = ""

    ex = paths.exchange
    bf_prec = config.bitfinex_price_precision if config.bitfinex_price_precision is not None else _env_bitfinex_price_precision()
    stats = ReplayStats(exchange=ex, day_dir=str(day_dir), segments_total=len(segments))
    last_ts_ms: Optional[int] = None

    for idx, seg in enumerate(segments):
        engine = _make_engine(ex, paths.depth, bf_prec)
        _adopt_snapshot(engine, ex, seg)
        stats.segments_replayed += 1

        gap_in_segment = False
        for payload in iter_diffs(paths.diff_path):
            recv_seq = int(payload.get("recv_seq", 0))
            if recv_seq <= seg.recv_seq:
                continue
            if seg.end_recv_seq is not None and recv_seq >= seg.end_recv_seq:
                break

            result = _feed_diff(engine, ex, payload)
            if result.action == "gap":
                stats.gaps += 1
                gap_in_segment = True
                if config.on_error == "best-effort":
                    stats.discontinuities += 1
                    if emit is not None:
                        emit(
                            {
                                "type": "discontinuity",
                                "exchange": ex,
                                "symbol": symbol,
                                "segment_index": seg.index,
                                "segment_tag": seg.tag,
                                "epoch_id": seg.epoch_id,
                                "recv_seq": recv_seq,
                                "reason": result.details,
                            }
                        )
                break

            if result.action in ("applied", "synced"):
                stats.diffs_applied += 1
                if config.validate_only:
                    continue
                frame = _frame_from_engine(engine, ex, symbol, seg, payload, config.top_n)
                new_last = _maybe_sleep(frame, config.speed, config.time_base, last_ts_ms)
                if new_last is not None:
                    last_ts_ms = new_last
                elif config.speed > 0:
                    # speed>0 but frame had no valid timestamp
                    last_ts_ms = None
                emit and emit(frame)
                stats.frames_emitted += 1

        if gap_in_segment and config.on_error == "strict" and idx == (len(segments) - 1):
            raise ReplayIntegrityError(f"Replay ended on invalid final segment {seg.index} ({seg.tag})")

    return stats


def emit_ndjson(stream, payload: dict[str, Any]) -> None:
    stream.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Deterministic orderbook replay for mm_recorder datasets")
    parser.add_argument("--day-dir", required=True, help="Path to data/<exchange>/<symbol>/<YYYYMMDD>")
    parser.add_argument("--exchange", default=None, help="Override exchange (binance|kraken|bitfinex)")
    parser.add_argument("--top-n", type=int, default=20, help="Number of levels to emit")
    parser.add_argument("--on-error", choices=["strict", "best-effort"], default="strict")
    parser.add_argument("--speed", type=float, default=0.0, help="Replay speed factor (0 = as fast as possible)")
    parser.add_argument("--time-base", choices=["recv", "event"], default="recv")
    parser.add_argument("--validate-only", action="store_true", help="Run validation without emitting frames")
    parser.add_argument("--bitfinex-price-precision", type=int, default=None, help="Bitfinex significant digits override")
    parser.add_argument("--out", default=None, help="Output NDJSON path (default: stdout)")
    args = parser.parse_args(argv)

    out_fh = None
    try:
        if args.out:
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_fh = out_path.open("w", encoding="utf-8")
            sink = lambda payload: emit_ndjson(out_fh, payload)
        elif args.validate_only:
            sink = None
        else:
            sink = lambda payload: emit_ndjson(sys.stdout, payload)

        stats = replay_orderbook_day(
            ReplayConfig(
                day_dir=Path(args.day_dir),
                exchange=args.exchange,
                top_n=max(1, int(args.top_n)),
                on_error=args.on_error,
                speed=float(args.speed),
                time_base=args.time_base,
                validate_only=bool(args.validate_only),
                bitfinex_price_precision=args.bitfinex_price_precision,
            ),
            emit=sink,
        )
        print(json.dumps({"type": "summary", **stats.as_dict()}), file=sys.stderr)
        return 0
    except (ReplayDataError, ReplayIntegrityError) as exc:
        print(f"replay_error: {exc}", file=sys.stderr)
        return 1
    finally:
        if out_fh is not None:
            out_fh.close()

