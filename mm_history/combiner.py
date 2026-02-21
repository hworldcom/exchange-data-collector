from __future__ import annotations

import csv
import gzip
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Sequence, Tuple

from mm_history.exchanges.base import HistoricalClient

from mm_history.types import Candle
from mm_core.symbols import symbol_fs as symbol_fs_fn

log = logging.getLogger(__name__)


_INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "3h": 10_800_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
    "1D": 86_400_000,
    "1W": 604_800_000,
    "14D": 1_209_600_000,
    # Month is variable; we treat it as 30 days for bucketing consistency.
    "1M": 2_592_000_000,
}


def interval_ms(interval: str) -> int:
    if interval not in _INTERVAL_MS:
        raise ValueError(f"Unsupported interval: {interval}")
    return _INTERVAL_MS[interval]


def bucket_start(ts_ms: int, interval: str) -> int:
    step = interval_ms(interval)
    return (ts_ms // step) * step


def read_candles_csv_gz(path: Path) -> Iterator[Candle]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield Candle(
                ts_ms=int(row["ts_ms"]),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                exchange=row["exchange"],
                symbol=row["symbol"],
                interval=row["interval"],
                raw=None,
            )


def read_trades_csv_gz(path: Path) -> Iterator[Dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def build_candles_from_trades(
    trades: Iterable[Dict[str, str]],
    interval: str,
    exchange: str,
    symbol: str,
) -> List[Candle]:
    buckets: Dict[int, Dict[str, str]] = {}
    step = interval_ms(interval)
    for row in trades:
        ts_ms = int(row.get("event_time_ms") or row.get("ts_ms") or row.get("time_ms") or 0)
        price = row.get("price") or row.get("p") or row.get("close")
        qty = row.get("qty") or row.get("q") or row.get("size") or "0"
        if price is None:
            continue
        bucket = (ts_ms // step) * step
        entry = buckets.get(bucket)
        if entry is None:
            buckets[bucket] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": qty,
            }
        else:
            entry["close"] = price
            entry["high"] = _fmt_number(max(float(entry["high"]), float(price)))
            entry["low"] = _fmt_number(min(float(entry["low"]), float(price)))
            entry["volume"] = str(float(entry["volume"]) + float(qty))

    candles: List[Candle] = []
    for bucket_ts in sorted(buckets.keys()):
        data = buckets[bucket_ts]
        candles.append(
            Candle(
                ts_ms=bucket_ts,
                open=data["open"],
                high=data["high"],
                low=data["low"],
                close=data["close"],
                volume=data["volume"],
                exchange=exchange,
                symbol=symbol,
                interval=interval,
                raw=None,
            )
        )
    return candles


def merge_candles(
    local: Iterable[Candle],
    remote: Iterable[Candle],
    interval: str,
) -> List[Candle]:
    merged: Dict[int, Candle] = {}

    for candle in remote:
        key = bucket_start(candle.ts_ms, interval)
        merged[key] = candle

    for candle in local:
        key = bucket_start(candle.ts_ms, interval)
        if key in merged:
            remote_candle = merged[key]
            if not _candles_equal(candle, remote_candle):
                log.error(
                    "Candle mismatch at %s local=%s remote=%s",
                    key,
                    asdict(candle),
                    asdict(remote_candle),
                )
        merged[key] = candle

    return [merged[key] for key in sorted(merged.keys())]


def _candles_equal(a: Candle, b: Candle) -> bool:
    return (
        str(a.open) == str(b.open)
        and str(a.high) == str(b.high)
        and str(a.low) == str(b.low)
        and str(a.close) == str(b.close)
        and str(a.volume) == str(b.volume)
    )


def _fmt_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return str(value)


def combine_from_sources(
    exchange: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    client: HistoricalClient,
    data_root: Path = Path("data"),
) -> List[Candle]:
    if end_ms <= start_ms:
        return []

    start_bucket = bucket_start(start_ms, interval)
    end_bucket = bucket_start(end_ms - 1, interval) + interval_ms(interval)

    symbol_norm = client.normalize_symbol(symbol)
    symbol_fs = symbol_fs_fn(symbol_norm)
    local_candles = list(
        _load_local_candles(
            data_root=data_root,
            exchange=exchange,
            symbol_fs=symbol_fs,
            interval=interval,
            start_ms=start_bucket,
            end_ms=end_bucket,
        )
    )
    local_by_bucket = {bucket_start(c.ts_ms, interval): c for c in local_candles}
    missing_ranges = _missing_ranges(start_bucket, end_bucket, interval, local_by_bucket)
    remote_candles: List[Candle] = []
    for window_start, window_end in missing_ranges:
        remote_candles.extend(
            _fetch_exchange_candles(
                client=client,
                symbol=symbol_norm,
                interval=interval,
                start_ms=window_start,
                end_ms=window_end,
            )
        )
    merged = merge_candles(local_candles, remote_candles, interval)
    return [c for c in merged if start_bucket <= c.ts_ms < end_bucket]


def _load_local_candles(
    data_root: Path,
    exchange: str,
    symbol_fs: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> Iterator[Candle]:
    base = data_root / exchange / symbol_fs
    if not base.exists():
        return iter(())
    for day_dir in sorted(p for p in base.iterdir() if p.is_dir()):
        history_dir = day_dir / "history"
        if not history_dir.exists():
            continue
        pattern = f"candles_{interval}_{symbol_fs}_*.csv.gz"
        for path in sorted(history_dir.glob(pattern)):
            for candle in read_candles_csv_gz(path):
                if start_ms <= candle.ts_ms < end_ms:
                    yield candle


def _missing_ranges(
    start_ms: int,
    end_ms: int,
    interval: str,
    local_by_bucket: Dict[int, Candle],
) -> List[Tuple[int, int]]:
    step = interval_ms(interval)
    cursor = bucket_start(start_ms, interval)
    missing: List[Tuple[int, int]] = []
    range_start = None
    while cursor < end_ms:
        if cursor not in local_by_bucket:
            if range_start is None:
                range_start = cursor
        else:
            if range_start is not None:
                missing.append((range_start, cursor))
                range_start = None
        cursor += step
    if range_start is not None:
        missing.append((range_start, end_ms))
    return missing


def _fetch_exchange_candles(
    client: HistoricalClient,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> List[Candle]:
    limit = client.max_candle_limit() or 1000
    if end_ms <= start_ms:
        return []
    # Binance endTime is inclusive; subtract 1ms to keep end-exclusive semantics.
    request_end_ms = end_ms - 1
    if request_end_ms < start_ms:
        request_end_ms = start_ms
    candles: List[Candle] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = list(
            client.fetch_candles(
                symbol=symbol,
                interval=interval,
                start_ms=cursor,
                end_ms=request_end_ms,
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
