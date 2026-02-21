from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from mm_history.exchanges.binance import BinanceHistoricalClient
from mm_history.types import Candle, Trade
from mm_history.writer import write_candles_csv, write_trades_ndjson
from mm_core.symbols import symbol_fs as symbol_fs_fn


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def _parse_ms(value: str) -> int:
    raw = int(value)
    if raw < 1_000_000_000_000:
        return raw * 1000
    return raw


def _day_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y%m%d")


def _resolve_client(exchange: str):
    if exchange == "binance":
        return BinanceHistoricalClient()
    raise SystemExit(f"Unsupported exchange for history: {exchange}")


def main() -> None:
    exchange = (_env("EXCHANGE") or "").lower()
    symbol = _env("SYMBOL")
    data_type = _env("TYPE")
    interval = _env("INTERVAL")
    start_ms_raw = _env("START_MS")
    end_ms_raw = _env("END_MS")
    limit_raw = _env("LIMIT")

    if not exchange or not symbol or not data_type:
        raise SystemExit("EXCHANGE, SYMBOL, TYPE are required")
    if data_type == "candles" and not interval:
        raise SystemExit("INTERVAL is required for candles")
    if not start_ms_raw or not end_ms_raw:
        raise SystemExit("START_MS and END_MS are required")

    start_ms = _parse_ms(start_ms_raw)
    end_ms = _parse_ms(end_ms_raw)
    if end_ms <= start_ms:
        raise SystemExit("END_MS must be greater than START_MS")

    client = _resolve_client(exchange)
    symbol_norm = client.normalize_symbol(symbol)
    limit = int(limit_raw) if limit_raw else client.max_candle_limit() or 1000

    if data_type == "candles":
        candles: List[Candle] = []
        cursor = start_ms
        while cursor < end_ms:
            batch = list(
                client.fetch_candles(
                    symbol=symbol_norm,
                    interval=interval,
                    start_ms=cursor,
                    end_ms=end_ms,
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

        if not candles:
            raise SystemExit("No candles returned")

        symbol_fs = symbol_fs_fn(symbol_norm)
        day_str = _day_str(start_ms)
        out_dir = Path("data") / exchange / symbol_fs / day_str / "history"
        out_path = out_dir / f"candles_{interval}_{symbol_fs}_{day_str}.csv.gz"
        write_candles_csv(out_path, candles)
        print(f"Wrote {len(candles)} candles to {out_path}")
        return

    if data_type == "trades":
        trades: List[Trade] = []
        cursor = start_ms
        last_id: int | None = None
        while cursor < end_ms:
            batch = list(
                client.fetch_trades(
                    symbol=symbol_norm,
                    start_ms=cursor,
                    end_ms=end_ms,
                    limit=limit,
                )
            )
            if not batch:
                break
            for trade in batch:
                if trade.trade_id is not None:
                    trade_id = int(trade.trade_id)
                    if last_id is not None and trade_id <= last_id:
                        continue
                    last_id = trade_id
                trades.append(trade)
            last_ts = batch[-1].ts_ms
            if last_ts <= cursor:
                break
            cursor = last_ts + 1

        if not trades:
            raise SystemExit("No trades returned")

        symbol_fs = symbol_fs_fn(symbol_norm)
        day_str = _day_str(start_ms)
        out_dir = Path("data") / exchange / symbol_fs / day_str / "history"
        out_path = out_dir / f"trades_{symbol_fs}_{day_str}.ndjson.gz"
        write_trades_ndjson(out_path, trades)
        print(f"Wrote {len(trades)} trades to {out_path}")
        return

    raise SystemExit("Unsupported TYPE. Use TYPE=candles or TYPE=trades")


if __name__ == "__main__":
    main()
