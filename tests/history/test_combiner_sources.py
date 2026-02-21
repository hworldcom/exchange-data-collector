from __future__ import annotations

import gzip
from pathlib import Path
from typing import Iterable, List

from mm_history.combiner import combine_from_sources, interval_ms
from mm_history.exchanges.base import HistoricalClient
from mm_history.types import Candle


class _FakeClient(HistoricalClient):
    name = "binance"

    def __init__(self, candles: List[Candle]) -> None:
        self._candles = candles

    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> Iterable[Candle]:
        return [
            c
            for c in self._candles
            if start_ms <= c.ts_ms < end_ms and c.interval == interval
        ]

    def fetch_trades(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ):
        return []

    def max_candle_limit(self) -> int:
        return 1000

    def normalize_symbol(self, symbol: str) -> str:
        return symbol


def _write_candles(path: Path, candles: List[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("ts_ms,open,high,low,close,volume,exchange,symbol,interval\n")
        for c in candles:
            fh.write(
                f"{c.ts_ms},{c.open},{c.high},{c.low},{c.close},{c.volume},{c.exchange},{c.symbol},{c.interval}\n"
            )


def test_combine_from_sources_prefers_local(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    interval = "1m"
    step = interval_ms(interval)
    t0 = 1_700_000_000_000
    t1 = t0 + step
    t2 = t1 + step

    local_candles = [
        Candle(t1, "10", "12", "9", "11", "5", "binance", "BTCUSDT", interval, None),
        Candle(t2, "11", "13", "10", "12", "6", "binance", "BTCUSDT", interval, None),
    ]
    remote_candles = [
        Candle(t0, "9", "11", "8", "10", "4", "binance", "BTCUSDT", interval, None),
        Candle(t1, "10", "99", "1", "11", "999", "binance", "BTCUSDT", interval, None),
        Candle(t2, "11", "13", "10", "12", "6", "binance", "BTCUSDT", interval, None),
    ]

    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260205" / "history"
    _write_candles(day_dir / "candles_1m_BTCUSDT_20260205.csv.gz", local_candles)

    client = _FakeClient(remote_candles)
    combined = combine_from_sources(
        exchange="binance",
        symbol="BTCUSDT",
        interval=interval,
        start_ms=t0,
        end_ms=t2 + step,
        client=client,
        data_root=Path("data"),
    )

    assert len(combined) == 3
    assert combined[0].ts_ms == t0
    assert combined[1].high == "12"  # local wins


def test_combine_from_sources_fills_gap(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    interval = "1m"
    step = interval_ms(interval)
    t0 = 1_700_000_000_000
    t1 = t0 + step
    t2 = t1 + step

    local_candles = [
        Candle(t0, "10", "12", "9", "11", "5", "binance", "BTCUSDT", interval, None),
        Candle(t2, "11", "13", "10", "12", "6", "binance", "BTCUSDT", interval, None),
    ]
    remote_candles = [
        Candle(t1, "10", "11", "9", "10", "4", "binance", "BTCUSDT", interval, None),
    ]

    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260205" / "history"
    _write_candles(day_dir / "candles_1m_BTCUSDT_20260205.csv.gz", local_candles)

    client = _FakeClient(remote_candles)
    combined = combine_from_sources(
        exchange="binance",
        symbol="BTCUSDT",
        interval=interval,
        start_ms=t0,
        end_ms=t2 + step,
        client=client,
        data_root=Path("data"),
    )

    assert [c.ts_ms for c in combined] == [t0, t1, t2]
