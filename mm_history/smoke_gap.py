from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

from mm_history.combiner import (
    bucket_start,
    combine_from_sources,
    interval_ms,
)
from mm_history.exchanges.binance import BinanceHistoricalClient
from mm_history.types import Candle


def _env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def _fetch_full_exchange(
    client: BinanceHistoricalClient,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> List[Candle]:
    limit = client.max_candle_limit() or 1000
    candles: List[Candle] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = list(
            client.fetch_candles(
                symbol=symbol,
                interval=interval,
                start_ms=cursor,
                end_ms=end_ms - 1,
                limit=limit,
            )
        )
        if not batch:
            break
        candles.extend(batch)
        last_ts = batch[-1].ts_ms
        if last_ts <= cursor:
            break
        cursor = last_ts + 1
    return candles


def _index_by_bucket(candles: List[Candle], interval: str) -> Dict[int, Candle]:
    return {bucket_start(c.ts_ms, interval): c for c in candles}


def main() -> None:
    exchange = (_env("EXCHANGE") or "binance").lower()
    symbol = _env("SYMBOL")
    interval = _env("INTERVAL") or "1s"
    start_ms = _env("START_MS")
    end_ms = _env("END_MS")
    data_root = Path(_env("DATA_ROOT", "data"))

    if not symbol or not start_ms or not end_ms:
        raise SystemExit("SYMBOL, START_MS, END_MS are required")
    if exchange != "binance":
        raise SystemExit("Only EXCHANGE=binance supported in smoke test")

    start_ms_i = int(start_ms)
    end_ms_i = int(end_ms)

    client = BinanceHistoricalClient()
    combined = combine_from_sources(
        exchange=exchange,
        symbol=symbol,
        interval=interval,
        start_ms=start_ms_i,
        end_ms=end_ms_i,
        client=client,
        data_root=data_root,
    )
    exchange_full = _fetch_full_exchange(
        client=client,
        symbol=client.normalize_symbol(symbol),
        interval=interval,
        start_ms=start_ms_i,
        end_ms=end_ms_i,
    )

    combined_idx = _index_by_bucket(combined, interval)
    exchange_idx = _index_by_bucket(exchange_full, interval)

    step = interval_ms(interval)
    cursor = bucket_start(start_ms_i, interval)
    end_bucket = bucket_start(end_ms_i - 1, interval)
    missing = 0
    mismatch = 0
    total = 0
    while cursor <= end_bucket:
        total += 1
        c = combined_idx.get(cursor)
        e = exchange_idx.get(cursor)
        if e is None:
            cursor += step
            continue
        if c is None:
            missing += 1
        else:
            if not (
                c.open == e.open
                and c.high == e.high
                and c.low == e.low
                and c.close == e.close
                and c.volume == e.volume
            ):
                mismatch += 1
        cursor += step

    print(f"Combined candles: {len(combined)}")
    print(f"Exchange candles: {len(exchange_full)}")
    print(f"Buckets checked: {total}")
    print(f"Missing vs exchange: {missing}")
    print(f"Mismatched OHLCV: {mismatch}")


if __name__ == "__main__":
    main()
