from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from datetime import datetime, timezone
import logging
from urllib.parse import parse_qsl
from typing import Dict, List, Optional

import websockets
from typing import Any

from mm_api.metrics_store import CloseSeries, compute_correlation, compute_returns, compute_volatility
from mm_api.protocols import make_message
from mm_api.sources import resolve_latest_paths, sanitize_exchange, sanitize_symbol
from mm_api.tailer import TailState, tail_text_ndjson
from mm_history.combiner import combine_from_sources, interval_ms
from mm_history.writer import write_candles_csv
from mm_history.exchanges.binance import BinanceHistoricalClient


POLL_INTERVAL_S = float(os.getenv("METRICS_POLL_INTERVAL_S", "1.0"))
METRICS_CACHE_HISTORY = os.getenv("METRICS_CACHE_HISTORY", "1").strip() in ("1", "true", "True")
log = logging.getLogger("mm_api.metrics")


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _parse_query(path: str) -> Dict[str, str]:
    if "?" not in path:
        return {}
    _, query = path.split("?", 1)
    items = parse_qsl(query, keep_blank_values=True)
    return {k: v for k, v in items if k}


def _parse_window_ms(value: str) -> int:
    if value.endswith("d"):
        return int(value[:-1]) * 24 * 60 * 60 * 1000
    if value.endswith("h"):
        return int(value[:-1]) * 60 * 60 * 1000
    if value.endswith("m"):
        return int(value[:-1]) * 60 * 1000
    if value.endswith("s"):
        return int(value[:-1]) * 1000
    return int(value)


def _build_series_from_candles(candles) -> CloseSeries:
    series = CloseSeries()
    for candle in candles:
        series.append(candle.ts_ms, float(candle.close))
    return series


async def _send_json(ws: Any, payload: Dict) -> None:
    await ws.send(json.dumps(payload, ensure_ascii=False))


async def _send_metric(
    ws: Any,
    exchange: str,
    symbols: List[str],
    interval: str,
    window_ms: int,
    metric: str,
    value: Optional[float],
) -> None:
    await _send_json(
        ws,
        make_message(
            msg_type="metric",
            exchange=exchange,
            symbol=",".join(symbols),
            ts_ms=_now_ms(),
            data={
                "metric": metric,
                "symbols": symbols,
                "interval": interval,
                "window_ms": window_ms,
                "value": value,
            },
        ),
    )


async def _metrics_loop(
    ws: Any,
    exchange: str,
    symbols: List[str],
    interval: str,
    window_ms: int,
    metric: str,
) -> None:
    exchange = sanitize_exchange(exchange)
    symbols = [sanitize_symbol(sym) for sym in symbols]
    if exchange != "binance":
        await _send_metric(ws, exchange, symbols, interval, window_ms, metric, None)
        return

    now_ms = _now_ms()
    start_ms = now_ms - window_ms
    client = BinanceHistoricalClient()
    candles_by_symbol: Dict[str, CloseSeries] = {}
    for symbol in symbols:
        candles = await asyncio.to_thread(
            combine_from_sources,
            exchange=exchange,
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=now_ms,
            client=client,
        )
        candles_by_symbol[symbol] = _build_series_from_candles(candles)
        if METRICS_CACHE_HISTORY and candles:
            await asyncio.to_thread(
                _cache_candles_by_day,
                exchange,
                symbol,
                interval,
                candles,
            )

    for series in candles_by_symbol.values():
        series.trim_before(start_ms)

    live_states = {symbol: TailState() for symbol in symbols}
    live_paths = {symbol: resolve_latest_paths(exchange, symbol).get("live_trades") for symbol in symbols}

    # Send initial metric right away (before live updates).
    if metric == "volatility":
        if len(symbols) != 1:
            value = None
        else:
            returns = compute_returns(candles_by_symbol[symbols[0]])
            value = compute_volatility(returns)
    elif metric == "correlation":
        if len(symbols) != 2:
            value = None
        else:
            r1 = compute_returns(candles_by_symbol[symbols[0]])
            r2 = compute_returns(candles_by_symbol[symbols[1]])
            value = compute_correlation(r1, r2)
    else:
        value = None
    await _send_metric(ws, exchange, symbols, interval, window_ms, metric, value)

    while True:
        await asyncio.sleep(POLL_INTERVAL_S)
        now_ms = _now_ms()
        start_ms = now_ms - window_ms
        for symbol in symbols:
            series = candles_by_symbol[symbol]
            series.trim_before(start_ms)
            live_path = live_paths.get(symbol)
            if live_path and live_path.exists():
                for payload in tail_text_ndjson(live_path, live_states[symbol]):
                    price = payload.get("price")
                    ts_ms = payload.get("event_time_ms") or payload.get("recv_ms")
                    if price is None or ts_ms is None:
                        continue
                    bucket = (int(ts_ms) // interval_ms(interval)) * interval_ms(interval)
                    series.append(bucket, float(price))

            # If no trade came in this second, keep last close (no update).

        if metric == "volatility":
            if len(symbols) != 1:
                value = None
            else:
                returns = compute_returns(candles_by_symbol[symbols[0]])
                value = compute_volatility(returns)
        elif metric == "correlation":
            if len(symbols) != 2:
                value = None
            else:
                r1 = compute_returns(candles_by_symbol[symbols[0]])
                r2 = compute_returns(candles_by_symbol[symbols[1]])
                value = compute_correlation(r1, r2)
        else:
            value = None

        await _send_metric(ws, exchange, symbols, interval, window_ms, metric, value)


def _get_path(ws: Any) -> str:
    req = getattr(ws, "request", None)
    if req is not None and hasattr(req, "path"):
        return req.path
    return getattr(ws, "path", "")


def _cache_candles_by_day(exchange: str, symbol: str, interval: str, candles: List) -> None:
    symbol_fs = symbol.replace("/", "").replace("-", "").replace(":", "").replace(" ", "")
    by_day: Dict[str, List] = {}
    for candle in candles:
        day = datetime.fromtimestamp(candle.ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d")
        by_day.setdefault(day, []).append(candle)
    for day, day_candles in by_day.items():
        out_dir = Path("data") / exchange / symbol_fs / day / "history"
        out_path = out_dir / f"candles_{interval}_{symbol_fs}_{day}.csv.gz"
        write_candles_csv(out_path, day_candles)
        log.info("Saved history candles %s (%s)", out_path, len(day_candles))


async def _handler(ws: Any) -> None:
    params = _parse_query(_get_path(ws))
    exchange = params.get("exchange", "binance")
    symbols = [s for s in params.get("symbols", "").split(",") if s]
    interval = params.get("interval", "1m")
    window = params.get("window", "180d")
    metric = params.get("metric", "correlation")
    log.info(
        "Metrics client connected exchange=%s symbols=%s interval=%s window=%s metric=%s",
        exchange,
        symbols,
        interval,
        window,
        metric,
    )
    if not symbols:
        await _send_metric(ws, exchange, [], interval, _parse_window_ms(window), metric, None)
        return
    await _send_metric(ws, exchange, symbols, interval, _parse_window_ms(window), "status", 1.0)
    window_ms = _parse_window_ms(window)
    try:
        await _metrics_loop(ws, exchange, symbols, interval, window_ms, metric)
    except ValueError as exc:
        await _send_metric(ws, exchange, symbols, interval, window_ms, "status", None)
        log.warning("Metrics invalid params: %s", exc)
    except websockets.ConnectionClosed:
        return


async def _run_server(host: str, port: int) -> None:
    async with websockets.serve(_handler, host, port):
        await asyncio.Future()


def main() -> None:
    host = os.getenv("METRICS_HOST", "0.0.0.0")
    port = int(os.getenv("METRICS_PORT", "8766"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    log.info("Metrics WS listening on ws://%s:%s/metrics", host, port)
    asyncio.run(_run_server(host, port))


if __name__ == "__main__":
    main()
