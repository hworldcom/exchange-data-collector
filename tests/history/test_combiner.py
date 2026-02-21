from __future__ import annotations

import gzip
from pathlib import Path

from mm_history.combiner import (
    bucket_start,
    build_candles_from_trades,
    interval_ms,
    merge_candles,
    read_candles_csv_gz,
)
from mm_history.types import Candle


def test_interval_ms_and_bucket_start() -> None:
    assert interval_ms("1s") == 1_000
    assert interval_ms("1m") == 60_000
    assert bucket_start(1700000123456, "1m") == 1700000100000


def test_build_candles_from_trades() -> None:
    trades = [
        {"event_time_ms": "1700000000000", "price": "10", "qty": "1"},
        {"event_time_ms": "1700000001000", "price": "12", "qty": "2"},
        {"event_time_ms": "1700000060000", "price": "11", "qty": "1"},
    ]
    candles = build_candles_from_trades(trades, "1m", "binance", "BTCUSDT")
    assert len(candles) == 2
    first = candles[0]
    assert first.open == "10"
    assert first.high == "12"
    assert first.low == "10"
    assert first.close == "12"
    assert float(first.volume) == 3.0


def test_merge_candles_prefers_local_and_logs(caplog) -> None:
    local = [
        Candle(
            ts_ms=1700000000000,
            open="10",
            high="12",
            low="10",
            close="11",
            volume="5",
            exchange="binance",
            symbol="BTCUSDT",
            interval="1m",
            raw=None,
        )
    ]
    remote = [
        Candle(
            ts_ms=1700000000123,
            open="10",
            high="13",
            low="9",
            close="11",
            volume="6",
            exchange="binance",
            symbol="BTCUSDT",
            interval="1m",
            raw=None,
        )
    ]
    caplog.set_level("ERROR")
    merged = merge_candles(local, remote, "1m")
    assert len(merged) == 1
    assert merged[0].high == "12"
    assert "Candle mismatch" in caplog.text


def test_read_candles_csv_gz(tmp_path: Path) -> None:
    path = tmp_path / "candles.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("ts_ms,open,high,low,close,volume,exchange,symbol,interval\n")
        fh.write("1700000000000,10,12,9,11,5,binance,BTCUSDT,1m\n")
    rows = list(read_candles_csv_gz(path))
    assert rows[0].close == "11"
