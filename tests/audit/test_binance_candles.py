from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from mm_audit.binance_candles import compare_binance_recorded_trades_to_history_candles
from mm_history.types import Candle


def _write_gz_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _make_day_dir(tmp_path: Path) -> Path:
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260221"
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "schema.json").write_text(
        json.dumps(
            {
                "schema_version": 4,
                "files": {
                    "events_csv": {"path": "events_BTCUSDT_20260221.csv.gz"},
                    "depth_diffs_ndjson_gz": {"path": "diffs/depth_diffs_BTCUSDT_20260221.ndjson.gz"},
                    "trades_ws_csv": {"path": "trades_ws_BTCUSDT_20260221.csv.gz"},
                },
            }
        ),
        encoding="utf-8",
    )
    _write_gz_csv(
        day_dir / "events_BTCUSDT_20260221.csv.gz",
        ["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"],
        [[1, 1, 1, 1, "run_start", 0, "{}"]],
    )
    (day_dir / "diffs").mkdir(parents=True, exist_ok=True)
    with gzip.open(day_dir / "diffs" / "depth_diffs_BTCUSDT_20260221.ndjson.gz", "wt", encoding="utf-8") as f:
        f.write("")
    _write_gz_csv(
        day_dir / "trades_ws_BTCUSDT_20260221.csv.gz",
        [
            "event_time_ms",
            "recv_time_ms",
            "recv_seq",
            "run_id",
            "trade_id",
            "trade_time_ms",
            "price",
            "qty",
            "is_buyer_maker",
            "side",
            "ord_type",
            "exchange",
            "symbol",
        ],
        [
            [1700000000001, 1700000000002, 10, 1, 1, 1700000000001, "100.0", "0.5", 0, "buy", "", "binance", "BTCUSDT"],
            [1700000001000, 1700000001001, 11, 1, 2, 1700000001000, "101.0", "0.25", 0, "buy", "", "binance", "BTCUSDT"],
            [1700000060000, 1700000060001, 12, 1, 3, 1700000060000, "99.0", "1.0", 1, "sell", "", "binance", "BTCUSDT"],
        ],
    )
    return day_dir


class _FakeBinanceClient:
    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").upper()

    def max_candle_limit(self) -> int:
        return 1000

    def fetch_candles(self, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int):
        assert symbol == "BTCUSDT"
        assert interval == "1m"
        candles = [
            Candle(
                ts_ms=1699999980000,  # minute bucket for 1700000000001
                open="100.0",
                high="101.0",
                low="100.0",
                close="101.0",
                volume="0.75",
                exchange="binance",
                symbol="BTCUSDT",
                interval="1m",
            ),
            Candle(
                ts_ms=1700000040000,  # minute bucket for 1700000060000
                open="99.0",
                high="99.0",
                low="99.0",
                close="99.0",
                volume="1.0",
                exchange="binance",
                symbol="BTCUSDT",
                interval="1m",
            ),
        ]
        # mimic pagination filtering
        return [c for c in candles if start_ms <= c.ts_ms < end_ms][:limit]


def test_compare_binance_recorded_trades_to_history_candles_ok(tmp_path: Path) -> None:
    day_dir = _make_day_dir(tmp_path)
    report = compare_binance_recorded_trades_to_history_candles(
        day_dir=day_dir,
        client=_FakeBinanceClient(),  # type: ignore[arg-type]
    )
    assert report.ok()
    assert report.minutes_recorded == 2
    assert report.minutes_compared == 2
    assert report.mismatches == 0


def test_compare_binance_recorded_trades_to_history_candles_mismatch(tmp_path: Path) -> None:
    day_dir = _make_day_dir(tmp_path)

    class BadClient(_FakeBinanceClient):
        def fetch_candles(self, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int):
            out = list(super().fetch_candles(symbol, interval, start_ms, end_ms, limit))
            out[0] = Candle(
                ts_ms=out[0].ts_ms,
                open=out[0].open,
                high=out[0].high,
                low=out[0].low,
                close="102.0",
                volume=out[0].volume,
                exchange=out[0].exchange,
                symbol=out[0].symbol,
                interval=out[0].interval,
            )
            return out

    report = compare_binance_recorded_trades_to_history_candles(
        day_dir=day_dir,
        client=BadClient(),  # type: ignore[arg-type]
    )
    assert not report.ok()
    assert report.mismatches == 1
    assert report.samples



def test_compare_binance_recorded_trades_to_history_candles_ignore_boundary_partials(tmp_path: Path) -> None:
    day_dir = _make_day_dir(tmp_path)

    class BoundaryClient(_FakeBinanceClient):
        def fetch_candles(self, symbol: str, interval: str, start_ms: int, end_ms: int, limit: int):
            out = list(super().fetch_candles(symbol, interval, start_ms, end_ms, limit))
            out[0] = Candle(
                ts_ms=out[0].ts_ms,
                open="99.5",
                high=out[0].high,
                low="99.5",
                close=out[0].close,
                volume="1.25",
                exchange=out[0].exchange,
                symbol=out[0].symbol,
                interval=out[0].interval,
            )
            out.append(
                Candle(
                    ts_ms=1700000100000,
                    open="98.0",
                    high="98.0",
                    low="98.0",
                    close="98.0",
                    volume="0.1",
                    exchange="binance",
                    symbol="BTCUSDT",
                    interval="1m",
                )
            )
            return [c for c in out if start_ms <= c.ts_ms <= end_ms][:limit]

    report = compare_binance_recorded_trades_to_history_candles(
        day_dir=day_dir,
        client=BoundaryClient(),  # type: ignore[arg-type]
    )
    assert not report.ok()
    assert report.mismatches == 1
    assert report.missing_in_recorded == 1

    report_ignored = compare_binance_recorded_trades_to_history_candles(
        day_dir=day_dir,
        client=BoundaryClient(),  # type: ignore[arg-type]
        ignore_boundary_partials=True,
    )
    assert report_ignored.ok()
    assert report_ignored.mismatches == 0
    assert report_ignored.missing_in_recorded == 0
    assert report_ignored.minutes_compared == 0
