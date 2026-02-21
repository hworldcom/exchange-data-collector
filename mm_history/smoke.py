from __future__ import annotations

import os
from pathlib import Path
from typing import List

from mm_history.combiner import (
    build_candles_from_trades,
    combine_from_sources,
    read_trades_csv_gz,
)
from mm_history.exchanges.binance import BinanceHistoricalClient
from mm_history.types import Candle
from mm_history.writer import write_candles_csv
from mm_core.symbols import symbol_fs as symbol_fs_fn


def _env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def _find_trades_files(day_dir: Path, symbol_fs: str) -> List[Path]:
    return sorted((day_dir).glob(f"trades_ws_{symbol_fs}_*.csv.gz"))


def build_local_candles_from_trades(
    exchange: str,
    symbol: str,
    interval: str,
    data_root: Path,
) -> List[Path]:
    symbol_fs = symbol_fs_fn(symbol)
    base = data_root / exchange / symbol_fs
    if not base.exists():
        return []

    written: List[Path] = []
    for day_dir in sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit()):
        trades_files = _find_trades_files(day_dir, symbol_fs)
        if not trades_files:
            continue
        trades_rows = []
        for trades_file in trades_files:
            trades_rows.extend(read_trades_csv_gz(trades_file))
        candles = build_candles_from_trades(trades_rows, interval, exchange, symbol)
        if not candles:
            continue
        history_dir = day_dir / "history"
        out_path = history_dir / f"candles_{interval}_{symbol_fs}_{day_dir.name}.csv.gz"
        write_candles_csv(out_path, candles)
        written.append(out_path)
    return written


def main() -> None:
    exchange = (_env("EXCHANGE") or "binance").lower()
    symbol = _env("SYMBOL")
    interval = _env("INTERVAL") or "1m"
    start_ms = _env("START_MS")
    end_ms = _env("END_MS")
    data_root = Path(_env("DATA_ROOT", "data"))

    if not symbol or not start_ms or not end_ms:
        raise SystemExit("SYMBOL, START_MS, END_MS are required")

    start_ms_i = int(start_ms)
    end_ms_i = int(end_ms)

    written = build_local_candles_from_trades(exchange, symbol, interval, data_root)
    if written:
        print(f"Wrote local candles: {len(written)} file(s)")
    else:
        print("No local candles written (no trades files found).")

    if exchange != "binance":
        raise SystemExit("Only EXCHANGE=binance supported in smoke test for now")

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
    if not combined:
        raise SystemExit("No combined candles returned")
    print(f"Combined candles: {len(combined)}")
    print(f"First ts_ms: {combined[0].ts_ms}  Last ts_ms: {combined[-1].ts_ms}")


if __name__ == "__main__":
    main()
