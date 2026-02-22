from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Optional

from mm_history.exchanges.binance import BinanceHistoricalClient
from mm_history.types import Candle
from mm_replay.reader import ReplayDataError, resolve_paths, iter_trades_csv


def _d(v: str | float | int | Decimal) -> Decimal:
    return v if isinstance(v, Decimal) else Decimal(str(v))


@dataclass
class MinuteAgg:
    ts_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    count: int = 0

    def apply(self, price: Decimal, qty: Decimal) -> None:
        if self.count == 0:
            self.open = self.high = self.low = self.close = price
            self.volume = qty
            self.count = 1
            return
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += qty
        self.count += 1


@dataclass
class BinanceCandleAuditReport:
    day_dir: str
    symbol: str
    minutes_recorded: int = 0
    minutes_historical: int = 0
    minutes_compared: int = 0
    missing_in_history: int = 0
    missing_in_recorded: int = 0
    mismatches: int = 0
    samples: list[dict[str, Any]] = field(default_factory=list)

    def ok(self) -> bool:
        return self.missing_in_history == 0 and self.mismatches == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "day_dir": self.day_dir,
            "symbol": self.symbol,
            "minutes_recorded": self.minutes_recorded,
            "minutes_historical": self.minutes_historical,
            "minutes_compared": self.minutes_compared,
            "missing_in_history": self.missing_in_history,
            "missing_in_recorded": self.missing_in_recorded,
            "mismatches": self.mismatches,
            "ok": self.ok(),
            "samples": self.samples,
        }


def _aggregate_recorder_trades_1m(trades_path: Path) -> dict[int, MinuteAgg]:
    out: dict[int, MinuteAgg] = {}
    for tr in iter_trades_csv(trades_path):
        minute_ts = (int(tr.trade_time_ms) // 60_000) * 60_000
        price = _d(tr.price)
        qty = _d(tr.qty)
        cur = out.get(minute_ts)
        if cur is None:
            cur = MinuteAgg(ts_ms=minute_ts, open=price, high=price, low=price, close=price, volume=Decimal("0"), count=0)
            out[minute_ts] = cur
        cur.apply(price, qty)
    return out


def _fetch_binance_1m_candles(
    *,
    client: BinanceHistoricalClient,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> dict[int, Candle]:
    candles: dict[int, Candle] = {}
    cursor = start_ms
    limit = client.max_candle_limit() or 1000
    while cursor < end_ms:
        batch = list(
            client.fetch_candles(
                symbol=symbol,
                interval="1m",
                start_ms=cursor,
                end_ms=end_ms,
                limit=limit,
            )
        )
        if not batch:
            break
        last_ts = cursor
        for c in batch:
            candles[int(c.ts_ms)] = c
            last_ts = max(last_ts, int(c.ts_ms))
        if last_ts < cursor:
            break
        cursor = last_ts + 60_000
    return candles


def compare_binance_recorded_trades_to_history_candles(
    *,
    day_dir: Path,
    symbol: str | None = None,
    client: BinanceHistoricalClient | None = None,
    tol: Decimal = Decimal("0"),
    max_samples: int = 10,
) -> BinanceCandleAuditReport:
    day_dir = Path(day_dir)
    paths = resolve_paths(day_dir, exchange="binance")
    if paths.trades_csv_path is None:
        raise ReplayDataError(f"No trades_ws csv found under {day_dir}")
    trades_path = paths.trades_csv_path
    recorder = _aggregate_recorder_trades_1m(trades_path)
    if not recorder:
        raise ReplayDataError(f"No trades found in {trades_path}")

    symbol_norm = (symbol or day_dir.parent.name).replace("/", "").upper()
    hist_client = client or BinanceHistoricalClient()
    symbol_norm = hist_client.normalize_symbol(symbol_norm)

    start_ms = min(recorder.keys())
    end_ms = max(recorder.keys()) + 60_000
    history = _fetch_binance_1m_candles(client=hist_client, symbol=symbol_norm, start_ms=start_ms, end_ms=end_ms)

    report = BinanceCandleAuditReport(day_dir=str(day_dir), symbol=symbol_norm)
    report.minutes_recorded = len(recorder)
    report.minutes_historical = len(history)

    all_minutes = sorted(set(recorder.keys()) | set(history.keys()))
    for ts in all_minutes:
        rec = recorder.get(ts)
        hist = history.get(ts)
        if rec is None:
            report.missing_in_recorded += 1
            continue
        if hist is None:
            report.missing_in_history += 1
            if len(report.samples) < max_samples:
                report.samples.append({"kind": "missing_in_history", "ts_ms": ts})
            continue

        report.minutes_compared += 1
        errors: list[str] = []
        for name, rv, hv in (
            ("open", rec.open, _d(hist.open)),
            ("high", rec.high, _d(hist.high)),
            ("low", rec.low, _d(hist.low)),
            ("close", rec.close, _d(hist.close)),
            ("volume", rec.volume, _d(hist.volume)),
        ):
            if abs(rv - hv) > tol:
                errors.append(f"{name} rec={rv} hist={hv}")
        if errors:
            report.mismatches += 1
            if len(report.samples) < max_samples:
                report.samples.append({"kind": "candle_mismatch", "ts_ms": ts, "errors": errors})

    return report


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Compare recorder trades against Binance historical 1m candles")
    parser.add_argument("--day-dir", required=True, help="Path to data/binance/<symbol>/<YYYYMMDD>")
    parser.add_argument("--symbol", default=None, help="Optional symbol override (default: infer from path)")
    parser.add_argument("--tol", default="0", help="Absolute Decimal tolerance for OHLCV comparisons")
    parser.add_argument("--max-samples", type=int, default=10)
    args = parser.parse_args(argv)

    try:
        report = compare_binance_recorded_trades_to_history_candles(
            day_dir=Path(args.day_dir),
            symbol=args.symbol,
            tol=_d(args.tol),
            max_samples=max(0, int(args.max_samples)),
        )
    except (ReplayDataError, ValueError) as exc:
        print(f"audit_error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"audit_error: {exc}", file=sys.stderr)
        return 3

    print(json.dumps({"type": "binance_trade_candle_audit", **report.as_dict()}, default=str))
    return 0 if report.ok() else 1


if __name__ == "__main__":
    raise SystemExit(main())

