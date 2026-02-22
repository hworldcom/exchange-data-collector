from __future__ import annotations

from pathlib import Path

from mm_audit.orderbook_trades import audit_orderbook_trade_consistency

from tests.replay.test_orderbook_replay import _make_binance_day, _write_orderbook_top1, _write_trades


def test_orderbook_trade_consistency_basic_ok(tmp_path: Path) -> None:
    day_dir = _make_binance_day(tmp_path, diffs=[])
    _write_orderbook_top1(
        day_dir,
        "BTCUSDT",
        [
            [1000, 1000, 10, 1, 0, "100.0", "1.0", "101.0", "1.0"],
            [1100, 1100, 20, 1, 0, "99.5", "1.0", "100.5", "1.0"],
        ],
    )
    _write_trades(
        day_dir,
        "BTCUSDT",
        [
            [1050, 1050, 15, 1, 1, 1050, "100.25", "0.1", 0, "buy", "", "binance", "BTCUSDT"],
            [1150, 1150, 25, 1, 2, 1150, "100.00", "0.2", 1, "sell", "", "binance", "BTCUSDT"],
        ],
    )

    report = audit_orderbook_trade_consistency(day_dir=day_dir, exchange="binance")

    assert report.crossed_books == 0
    assert report.empty_top_books == 0
    assert report.trades_without_prev_book == 0
    assert report.trades_outside_prev_top == 0
    assert report.trades_outside_prev_next_envelope == 0
    assert report.trades_outside_prev_next_top20_envelope == 0


def test_orderbook_trade_consistency_flags_crossed_and_outside_trade(tmp_path: Path) -> None:
    day_dir = _make_binance_day(tmp_path, diffs=[])
    _write_orderbook_top1(
        day_dir,
        "BTCUSDT",
        [
            [1000, 1000, 10, 1, 0, "100.0", "1.0", "99.0", "1.0"],  # crossed
            [1100, 1100, 20, 1, 0, "100.0", "1.0", "101.0", "1.0"],
        ],
    )
    _write_trades(
        day_dir,
        "BTCUSDT",
        [
            [1050, 1050, 15, 1, 1, 1050, "150.00", "0.1", 0, "buy", "", "binance", "BTCUSDT"],
        ],
    )

    report = audit_orderbook_trade_consistency(day_dir=day_dir, exchange="binance")

    assert report.crossed_books == 1
    assert report.trades_outside_prev_top >= 1
    assert report.trades_outside_prev_next_envelope >= 1
    assert report.trades_outside_prev_next_top20_envelope >= 1
    assert report.samples
