from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Iterable
import csv
import gzip
import json

from mm_history.types import Candle, Trade


def _open_gzip_text(path: Path, mode: str = "wt"):
    path.parent.mkdir(parents=True, exist_ok=True)
    return gzip.open(path, mode, encoding="utf-8")


def write_candles_csv(path: Path, candles: Iterable[Candle]) -> None:
    with _open_gzip_text(path, mode="wt") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "ts_ms",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "exchange",
                "symbol",
                "interval",
            ],
        )
        writer.writeheader()
        for candle in candles:
            writer.writerow(
                {
                    "ts_ms": candle.ts_ms,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "exchange": candle.exchange,
                    "symbol": candle.symbol,
                    "interval": candle.interval,
                }
            )


def write_trades_ndjson(path: Path, trades: Iterable[Trade]) -> None:
    with _open_gzip_text(path, mode="wt") as fh:
        for trade in trades:
            payload = asdict(trade)
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
