from __future__ import annotations

import asyncio
import json
import os
import logging
import time
from urllib.parse import parse_qsl
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import websockets
from typing import Any

from mm_api.protocols import make_message
from mm_api.sources import resolve_latest_paths
from mm_api.tailer import (
    TailState,
    count_gzip_lines,
    count_text_lines,
    tail_csv,
    tail_ndjson,
    tail_text_ndjson,
)


POLL_INTERVAL_S = float(os.getenv("WS_RELAY_POLL_INTERVAL_S", "1.0"))
LIVE_ONLY = os.getenv("WS_RELAY_LIVE_ONLY", "0").strip() in ("1", "true", "True")
LEVELS_N = int(os.getenv("WS_RELAY_LEVELS", "20"))
LEVELS_INTERVAL_S = float(os.getenv("WS_RELAY_LEVELS_INTERVAL_S", "1.0"))
VOLUME_WINDOW_S = int(os.getenv("WS_RELAY_VOLUME_WINDOW_S", str(24 * 60 * 60)))
VOLUME_INTERVAL_S = float(os.getenv("WS_RELAY_VOLUME_INTERVAL_S", "1.0"))
log = logging.getLogger("mm_api.relay")


class _TopOfBook:
    def __init__(self) -> None:
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self.best_bid: float | None = None
        self.best_ask: float | None = None

    def seed(self, bids: list, asks: list) -> None:
        for price, qty in bids:
            self._set_level(self._bids, price, qty, is_bid=True)
        for price, qty in asks:
            self._set_level(self._asks, price, qty, is_bid=False)
        self._recompute_best()

    def apply_updates(self, bids: list, asks: list) -> None:
        for price, qty in bids:
            self._set_level(self._bids, price, qty, is_bid=True)
        for price, qty in asks:
            self._set_level(self._asks, price, qty, is_bid=False)
        self._adjust_best()

    def _set_level(self, book: dict[float, float], price: str | float, qty: str | float, is_bid: bool) -> None:
        p = float(price)
        q = float(qty)
        if q <= 0:
            book.pop(p, None)
            if is_bid and self.best_bid == p:
                self.best_bid = None
            if (not is_bid) and self.best_ask == p:
                self.best_ask = None
            return
        book[p] = q
        if is_bid:
            if self.best_bid is None or p > self.best_bid:
                self.best_bid = p
        else:
            if self.best_ask is None or p < self.best_ask:
                self.best_ask = p

    def _recompute_best(self) -> None:
        self.best_bid = max(self._bids.keys()) if self._bids else None
        self.best_ask = min(self._asks.keys()) if self._asks else None

    def _adjust_best(self) -> None:
        if self.best_bid is None and self._bids:
            self.best_bid = max(self._bids.keys())
        if self.best_ask is None and self._asks:
            self.best_ask = min(self._asks.keys())

    def top_levels(self, n: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self._bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self._asks.items(), key=lambda x: x[0])[:n]
        return bids, asks


class _RollingVolume:
    def __init__(self, window_s: int) -> None:
        self.window_s = window_s
        self._buckets: dict[int, dict[str, float]] = {}
        self._total_buy = 0.0
        self._total_sell = 0.0

    def add(self, ts_ms: int, qty: float, side: str | None) -> None:
        sec = int(ts_ms // 1000)
        bucket = self._buckets.get(sec)
        if bucket is None:
            bucket = {"buy": 0.0, "sell": 0.0}
            self._buckets[sec] = bucket
        if side == "buy":
            bucket["buy"] += qty
            self._total_buy += qty
        elif side == "sell":
            bucket["sell"] += qty
            self._total_sell += qty
        else:
            # Unknown side, treat as total volume only (ignored for buy/sell split).
            pass
        self._evict(sec)

    def _evict(self, now_sec: int) -> None:
        cutoff = now_sec - self.window_s + 1
        to_delete = [sec for sec in self._buckets.keys() if sec < cutoff]
        for sec in to_delete:
            bucket = self._buckets.pop(sec)
            self._total_buy -= bucket["buy"]
            self._total_sell -= bucket["sell"]

    def totals(self) -> tuple[float, float]:
        return self._total_buy, self._total_sell


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _parse_query(path: str) -> Dict[str, str]:
    if "?" not in path:
        return {}
    _, query = path.split("?", 1)
    items = parse_qsl(query, keep_blank_values=True)
    return {k: v for k, v in items if k}


async def _send_json(ws: Any, payload: Dict[str, Any]) -> None:
    await ws.send(json.dumps(payload, ensure_ascii=False))


async def _send_status(ws: Any, exchange: str, symbol: str, message: str) -> None:
    await _send_json(
        ws,
        make_message(
            msg_type="status",
            exchange=exchange,
            symbol=symbol,
            ts_ms=_now_ms(),
            data={"message": message},
        ),
    )


def _load_snapshot_data(path: Optional[str]) -> Optional[dict]:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return raw if isinstance(raw, dict) else {"raw": raw}
    except Exception:
        return None


async def _send_snapshot(ws: Any, exchange: str, symbol: str, data: Optional[dict]) -> None:
    if data is None:
        await _send_status(ws, exchange, symbol, "snapshot not found")
        return
    await _send_json(
        ws,
        make_message(
            msg_type="snapshot",
            exchange=exchange,
            symbol=symbol,
            ts_ms=_now_ms(),
            data=data,
        ),
    )


async def _stream_loop(
    ws: Any,
    exchange: str,
    symbol: str,
    from_mode: str,
) -> None:
    paths = resolve_latest_paths(exchange, symbol)
    if not paths:
        await _send_status(ws, exchange, symbol, "no data directory found")
        return

    snapshot_data = _load_snapshot_data(str(paths.get("snapshot")) if paths.get("snapshot") else None)
    await _send_snapshot(ws, exchange, symbol, snapshot_data)

    diff_state = TailState()
    trade_state = TailState()
    event_state = TailState()
    book = _TopOfBook()
    last_best = (None, None)
    last_levels_emit = 0.0
    last_volume_emit = 0.0
    volume = _RollingVolume(VOLUME_WINDOW_S)
    if snapshot_data:
        bids = snapshot_data.get("bids") or snapshot_data.get("b") or []
        asks = snapshot_data.get("asks") or snapshot_data.get("a") or []
        if bids or asks:
            book.seed(bids, asks)
    diff_path = paths.get("live_diffs") if LIVE_ONLY else (paths.get("live_diffs") or paths.get("diffs"))
    trade_path = paths.get("live_trades") if LIVE_ONLY else (paths.get("live_trades") or paths.get("trades"))
    current_day_dir = paths.get("day_dir")
    if from_mode == "tail":
        for path, state in (
            (diff_path, diff_state),
            (trade_path, trade_state),
            (None if LIVE_ONLY else paths.get("events"), event_state),
        ):
            if path:
                if path.suffix == ".gz":
                    state.line_index = count_gzip_lines(path)
                else:
                    state.line_index = count_text_lines(path)

    await _send_status(ws, exchange, symbol, "tailing latest files")

    while True:
        await asyncio.sleep(POLL_INTERVAL_S)
        latest_paths = resolve_latest_paths(exchange, symbol)
        if latest_paths and latest_paths.get("day_dir") != current_day_dir:
            current_day_dir = latest_paths.get("day_dir")
            paths = latest_paths
            diff_path = paths.get("live_diffs") if LIVE_ONLY else (paths.get("live_diffs") or paths.get("diffs"))
            trade_path = paths.get("live_trades") if LIVE_ONLY else (paths.get("live_trades") or paths.get("trades"))
            diff_state = TailState()
            trade_state = TailState()
            event_state = TailState()
            book = _TopOfBook()
            last_best = (None, None)
            if from_mode == "tail":
                for path, state in (
                    (diff_path, diff_state),
                    (trade_path, trade_state),
                    (None if LIVE_ONLY else paths.get("events"), event_state),
                ):
                    if path:
                        if path.suffix == ".gz":
                            state.line_index = count_gzip_lines(path)
                        else:
                            state.line_index = count_text_lines(path)
            await _send_status(ws, exchange, symbol, f"switched to new day folder {current_day_dir}")
            snapshot_data = _load_snapshot_data(
                str(paths.get("snapshot")) if paths.get("snapshot") else None
            )
            if snapshot_data:
                bids = snapshot_data.get("bids") or snapshot_data.get("b") or []
                asks = snapshot_data.get("asks") or snapshot_data.get("a") or []
                if bids or asks:
                    book.seed(bids, asks)
            await _send_snapshot(
                ws,
                exchange,
                symbol,
                snapshot_data,
            )
        if diff_path:
            tailer = tail_ndjson if diff_path.suffix == ".gz" else tail_text_ndjson
            for payload in tailer(diff_path, diff_state):
                bids = payload.get("b") or []
                asks = payload.get("a") or []
                if bids or asks:
                    book.apply_updates(bids, asks)
                    if book.best_bid is not None and book.best_ask is not None:
                        if (book.best_bid, book.best_ask) != last_best:
                            mid = (book.best_bid + book.best_ask) / 2
                            spread_abs = book.best_ask - book.best_bid
                            spread_bps = (spread_abs / mid) * 10_000 if mid > 0 else 0.0
                            await _send_json(
                                ws,
                                make_message(
                                    msg_type="spread",
                                    exchange=exchange,
                                    symbol=symbol,
                                    ts_ms=int(payload.get("E") or payload.get("recv_ms") or _now_ms()),
                                    data={
                                        "bid": book.best_bid,
                                        "ask": book.best_ask,
                                        "mid": mid,
                                        "spread_abs": spread_abs,
                                        "spread_bps": spread_bps,
                                    },
                                ),
                            )
                            last_best = (book.best_bid, book.best_ask)
                await _send_json(
                    ws,
                    make_message(
                        msg_type="diff",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=int(payload.get("E") or payload.get("recv_ms") or _now_ms()),
                        data=payload,
                    ),
                )
        now_s = time.time()
        if now_s - last_levels_emit >= LEVELS_INTERVAL_S:
            bids, asks = book.top_levels(LEVELS_N)
            if bids or asks:
                sum_bid = sum(q for _, q in bids)
                sum_ask = sum(q for _, q in asks)
                await _send_json(
                    ws,
                    make_message(
                        msg_type="levels",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=_now_ms(),
                        data={
                            "levels": LEVELS_N,
                            "bids": bids,
                            "asks": asks,
                            "sum_bid_qty": sum_bid,
                            "sum_ask_qty": sum_ask,
                        },
                    ),
                )
            last_levels_emit = now_s
        if trade_path:
            tailer = tail_ndjson if trade_path.suffix == ".gz" else tail_text_ndjson
            for payload in tailer(trade_path, trade_state):
                side = payload.get("side")
                qty = payload.get("qty")
                ts_ms = payload.get("event_time_ms") or payload.get("recv_ms")
                if qty is not None and ts_ms is not None:
                    try:
                        volume.add(int(ts_ms), float(qty), side)
                    except Exception:
                        log.exception("Failed to update rolling volume")
                await _send_json(
                    ws,
                    make_message(
                        msg_type="trade",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=int(payload.get("E") or payload.get("recv_ms") or _now_ms()),
                        data=payload,
                    ),
                )
        now_s = time.time()
        if now_s - last_volume_emit >= VOLUME_INTERVAL_S:
            buy_vol, sell_vol = volume.totals()
            total_vol = buy_vol + sell_vol
            await _send_json(
                ws,
                make_message(
                    msg_type="volume_24h",
                    exchange=exchange,
                    symbol=symbol,
                    ts_ms=_now_ms(),
                    data={
                        "window_s": VOLUME_WINDOW_S,
                        "buy_volume": buy_vol,
                        "sell_volume": sell_vol,
                        "total_volume": total_vol,
                    },
                ),
            )
            last_volume_emit = now_s
        if (not LIVE_ONLY) and paths.get("events"):
            for payload in tail_csv(paths["events"], event_state):
                await _send_json(
                    ws,
                    make_message(
                        msg_type="event",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=int(payload.get("recv_ms") or _now_ms()),
                        data=payload,
                    ),
                )


def _get_path(ws: Any) -> str:
    req = getattr(ws, "request", None)
    if req is not None and hasattr(req, "path"):
        return req.path
    return getattr(ws, "path", "")


async def _handler(ws: Any) -> None:
    params = _parse_query(_get_path(ws))
    exchange = params.get("exchange", "binance")
    symbol = params.get("symbol")
    from_mode = params.get("from", "tail")
    if not symbol:
        await _send_status(ws, exchange, "", "symbol is required")
        return
    log.info("Relay client connected exchange=%s symbol=%s from=%s", exchange, symbol, from_mode)
    await _send_status(ws, exchange, symbol, "connected")
    try:
        await _stream_loop(ws, exchange, symbol, from_mode)
    except ValueError as exc:
        await _send_status(ws, exchange, symbol, f"invalid params: {exc}")
    except websockets.ConnectionClosed:
        return


async def _run_server(host: str, port: int) -> None:
    async with websockets.serve(_handler, host, port):
        await asyncio.Future()


def main() -> None:
    host = os.getenv("WS_RELAY_HOST", "0.0.0.0")
    port = int(os.getenv("WS_RELAY_PORT", "8765"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    log.info("WS relay listening on ws://%s:%s/ws", host, port)
    asyncio.run(_run_server(host, port))


if __name__ == "__main__":
    main()
