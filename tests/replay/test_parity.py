from __future__ import annotations

from pathlib import Path

from mm_replay.parity import compare_replay_to_recorded_orderbook

from tests.replay.test_orderbook_replay import _make_binance_day, _write_orderbook_top1


def test_parity_matches_replayed_orderbook_rows(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 101, "u": 101, "b": [["100.00", "1.50"]], "a": []},
            {"recv_ms": 1200, "recv_seq": 12, "E": 1200, "U": 102, "u": 102, "b": [], "a": [["101.00", "1.75"]]},
        ],
    )
    _write_orderbook_top1(
        day_dir,
        "BTCUSDT",
        [
            [1100, 1100, 11, 1, 0, "100.00000000", "1.50000000", "101.00000000", "2.00000000"],
            [1200, 1200, 12, 1, 0, "100.00000000", "1.50000000", "101.00000000", "1.75000000"],
        ],
    )

    report = compare_replay_to_recorded_orderbook(day_dir=day_dir, exchange="binance", top_n=1)

    assert report.ok()
    assert report.compared_rows == 2
    assert report.recorded_rows == 2
    assert report.replay_book_frames == 2


def test_parity_reports_mismatch(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 101, "u": 101, "b": [["100.00", "1.50"]], "a": []},
        ],
    )
    _write_orderbook_top1(
        day_dir,
        "BTCUSDT",
        [
            [1100, 1100, 11, 1, 0, "100.00000000", "9.99000000", "101.00000000", "2.00000000"],
        ],
    )

    report = compare_replay_to_recorded_orderbook(day_dir=day_dir, exchange="binance", top_n=1)

    assert not report.ok()
    assert report.mismatches == 1
    assert report.samples


def test_parity_ignores_trailing_window_end_depth_without_orderbook_row(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 101, "u": 101, "b": [["100.00", "1.50"]], "a": []},
            # Recorder may persist this diff but skip topbook row because window_end triggers before write_topn().
            {"recv_ms": 1200, "recv_seq": 20, "E": 1200, "U": 102, "u": 102, "b": [["100.00", "1.75"]], "a": []},
        ],
        events_rows=[
            [1, 1000, 10, 1, "snapshot_loaded", 0, "{\"tag\": \"initial\", \"path\": \"snapshots/snapshot_000001_initial.csv\"}"],
            [2, 1201, 21, 1, "window_end", 0, "{\"end\": \"2026-02-22T00:15:00+01:00\"}"],
            [3, 1202, 22, 1, "run_stop", 0, "{\"symbol\": \"BTCUSDT\"}"],
        ],
    )
    _write_orderbook_top1(
        day_dir,
        "BTCUSDT",
        [
            [1100, 1100, 11, 1, 0, "100.00000000", "1.50000000", "101.00000000", "2.00000000"],
        ],
    )

    report = compare_replay_to_recorded_orderbook(day_dir=day_dir, exchange="binance", top_n=1)

    assert report.ok()
    assert report.extra_replay_frames == 0
    assert report.samples[-1]["kind"] == "ignored_window_end_trailing_depth"
