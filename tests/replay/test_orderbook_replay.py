from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

import pytest

from mm_core.checksum.kraken import KrakenBook
from mm_replay.orderbook import ReplayConfig, ReplayIntegrityError, replay_orderbook_day
from mm_replay.reader import ReplayDataError


def _write_schema(day_dir: Path, symbol_fs: str) -> None:
    _write_schema_version(day_dir, symbol_fs, schema_version=4)


def _write_schema_version(day_dir: Path, symbol_fs: str, *, schema_version: int) -> None:
    schema = {
        "schema_version": schema_version,
        "files": {
            "events_csv": {"path": f"events_{symbol_fs}_20260221.csv.gz"},
            "depth_diffs_ndjson_gz": {
                "path": f"diffs/depth_diffs_{symbol_fs}_20260221.ndjson.gz",
                "depth": 20,
            },
            "orderbook_ws_depth_csv": {
                "path": f"orderbook_ws_depth_{symbol_fs}_20260221.csv.gz",
                "format": "csv",
                "compression": "gzip",
            },
            "trades_ws_csv": {
                "path": f"trades_ws_{symbol_fs}_20260221.csv.gz",
                "format": "csv",
                "compression": "gzip",
            },
        },
    }
    (day_dir / "schema.json").write_text(json.dumps(schema), encoding="utf-8")


def _write_events(day_dir: Path, symbol_fs: str, rows: list[list[object]]) -> None:
    path = day_dir / f"events_{symbol_fs}_20260221.csv.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"])
        w.writerows(rows)


def _write_snapshot(day_dir: Path, name: str, last_update_id: int = 100) -> None:
    path = day_dir / "snapshots" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "event_id", "side", "price", "qty", "lastUpdateId"])
        w.writerow([1, 1, "bid", "100.00", "1.00", last_update_id])
        w.writerow([1, 1, "ask", "101.00", "2.00", last_update_id])


def _write_diffs(day_dir: Path, symbol_fs: str, payloads: list[dict]) -> None:
    path = day_dir / "diffs" / f"depth_diffs_{symbol_fs}_20260221.ndjson.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for payload in payloads:
            f.write(json.dumps(payload) + "\n")


def _write_trades(day_dir: Path, symbol_fs: str, rows: list[list[object]]) -> None:
    path = day_dir / f"trades_ws_{symbol_fs}_20260221.csv.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
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
            ]
        )
        w.writerows(rows)


def _write_trades_v2(day_dir: Path, symbol_fs: str, rows: list[list[object]]) -> None:
    path = day_dir / f"trades_ws_{symbol_fs}_20260221.csv.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
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
            ]
        )
        w.writerows(rows)


def _write_orderbook_top1(day_dir: Path, symbol_fs: str, rows: list[list[object]]) -> None:
    path = day_dir / f"orderbook_ws_depth_{symbol_fs}_20260221.csv.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "event_time_ms",
                "recv_time_ms",
                "recv_seq",
                "run_id",
                "epoch_id",
                "bid1_price",
                "bid1_qty",
                "ask1_price",
                "ask1_qty",
            ]
        )
        w.writerows(rows)


def _make_binance_day(tmp_path: Path, *, diffs: list[dict], events_rows: list[list[object]] | None = None) -> Path:
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260221"
    day_dir.mkdir(parents=True, exist_ok=True)
    _write_schema(day_dir, "BTCUSDT")
    _write_snapshot(day_dir, "snapshot_000001_initial.csv", last_update_id=100)
    if events_rows is None:
        events_rows = [
            [
                1,
                1000,
                10,
                1,
                "snapshot_loaded",
                0,
                json.dumps({"tag": "initial", "path": "snapshots/snapshot_000001_initial.csv"}),
            ]
        ]
    _write_events(day_dir, "BTCUSDT", events_rows)
    _write_diffs(day_dir, "BTCUSDT", diffs)
    _write_trades(day_dir, "BTCUSDT", [])
    return day_dir


def test_replay_binance_emits_book_frames(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {
                "recv_ms": 1100,
                "recv_seq": 11,
                "E": 1100,
                "U": 101,
                "u": 101,
                "b": [["100.00", "1.50"]],
                "a": [],
            },
            {
                "recv_ms": 1200,
                "recv_seq": 12,
                "E": 1200,
                "U": 102,
                "u": 102,
                "b": [],
                "a": [["101.00", "1.75"]],
            },
        ],
    )

    frames: list[dict] = []
    stats = replay_orderbook_day(ReplayConfig(day_dir=day_dir, exchange="binance", top_n=1), emit=frames.append)

    assert stats.segments_total == 1
    assert stats.diffs_applied == 2
    assert stats.gaps == 0
    assert stats.frames_emitted == 2
    assert [f["type"] for f in frames] == ["book", "book"]
    assert frames[0]["best_bid"] == 100.0
    assert frames[0]["best_ask"] == 101.0
    assert frames[0]["bids"][0] == [100.0, 1.5]
    assert frames[1]["asks"][0] == [101.0, 1.75]


def test_replay_binance_bridges_snapshot_before_emitting(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            # Stale event should not emit a frame.
            {
                "recv_ms": 1050,
                "recv_seq": 11,
                "E": 1050,
                "U": 90,
                "u": 100,
                "b": [["100.00", "9.99"]],
                "a": [],
            },
            # First bridge event: U <= lastUpdateId+1 <= u (100 <= 101 <= 101)
            {
                "recv_ms": 1100,
                "recv_seq": 12,
                "E": 1100,
                "U": 100,
                "u": 101,
                "b": [["100.00", "1.25"]],
                "a": [],
            },
            {
                "recv_ms": 1200,
                "recv_seq": 13,
                "E": 1200,
                "U": 102,
                "u": 102,
                "b": [],
                "a": [["101.00", "1.50"]],
            },
        ],
    )

    frames: list[dict] = []
    stats = replay_orderbook_day(ReplayConfig(day_dir=day_dir, exchange="binance", top_n=1), emit=frames.append)

    assert stats.diffs_applied == 2
    assert stats.frames_emitted == 2
    assert [f["recv_seq"] for f in frames] == [12, 13]
    assert frames[0]["bids"][0] == [100.0, 1.25]
    assert frames[1]["asks"][0] == [101.0, 1.5]


def test_replay_strict_raises_on_final_gap(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {
                "recv_ms": 1100,
                "recv_seq": 11,
                "E": 1100,
                "U": 150,
                "u": 150,
                "b": [["100.00", "1.50"]],
                "a": [],
            }
        ],
    )

    with pytest.raises(ReplayIntegrityError):
        replay_orderbook_day(ReplayConfig(day_dir=day_dir, exchange="binance", on_error="strict"))


def test_replay_strict_continues_after_resync_recovery(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            # First segment invalid (gap)
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 150, "u": 150, "b": [], "a": []},
            # Second segment valid after resync
            {"recv_ms": 2100, "recv_seq": 21, "E": 2100, "U": 201, "u": 201, "b": [["100.00", "2.00"]], "a": []},
        ],
        events_rows=[
            [
                1,
                1000,
                10,
                1,
                "snapshot_loaded",
                0,
                json.dumps({"tag": "initial", "path": "snapshots/snapshot_000001_initial.csv"}),
            ],
            [2, 1500, 20, 1, "resync_start", 1, json.dumps({"tag": "resync_000001"})],
            [
                3,
                2000,
                20,
                1,
                "snapshot_loaded",
                1,
                json.dumps({"tag": "resync_000001", "path": "snapshots/snapshot_000003_resync_000001.csv"}),
            ],
        ],
    )
    _write_snapshot(day_dir, "snapshot_000003_resync_000001.csv", last_update_id=200)

    frames: list[dict] = []
    stats = replay_orderbook_day(
        ReplayConfig(day_dir=day_dir, exchange="binance", on_error="strict", top_n=1),
        emit=frames.append,
    )

    assert stats.gaps == 1
    assert stats.discontinuities == 0
    assert stats.diffs_applied == 1
    assert [f["type"] for f in frames] == ["book"]
    assert frames[0]["recv_seq"] == 21


def test_replay_best_effort_emits_discontinuity_and_continues(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 150, "u": 150, "b": [], "a": []},
            {"recv_ms": 2100, "recv_seq": 21, "E": 2100, "U": 201, "u": 201, "b": [["100.00", "2.00"]], "a": []},
        ],
        events_rows=[
            [
                1,
                1000,
                10,
                1,
                "snapshot_loaded",
                0,
                json.dumps({"tag": "initial", "path": "snapshots/snapshot_000001_initial.csv"}),
            ],
            [2, 1500, 20, 1, "resync_start", 1, json.dumps({"tag": "resync_000001"})],
            [
                3,
                2000,
                20,
                1,
                "snapshot_loaded",
                1,
                json.dumps({"tag": "resync_000001", "path": "snapshots/snapshot_000003_resync_000001.csv"}),
            ],
        ],
    )
    _write_snapshot(day_dir, "snapshot_000003_resync_000001.csv", last_update_id=200)

    frames: list[dict] = []
    stats = replay_orderbook_day(
        ReplayConfig(day_dir=day_dir, exchange="binance", on_error="best-effort", top_n=1),
        emit=frames.append,
    )

    assert stats.gaps == 1
    assert stats.discontinuities == 1
    assert stats.diffs_applied == 1
    assert [f["type"] for f in frames] == ["discontinuity", "book"]
    assert frames[-1]["recv_seq"] == 21


def test_replay_binance_interleaves_trade_frames_by_recv_seq(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 101, "u": 101, "b": [["100.00", "1.10"]], "a": []},
            {"recv_ms": 1300, "recv_seq": 13, "E": 1300, "U": 102, "u": 102, "b": [], "a": [["101.00", "1.90"]]},
        ],
    )
    _write_trades(
        day_dir,
        "BTCUSDT",
        [
            [1200, 1200, 12, 1, 999, 1200, "100.50", "0.25", 0, "buy", "", "binance", "BTCUSDT"],
        ],
    )

    frames: list[dict] = []
    stats = replay_orderbook_day(
        ReplayConfig(day_dir=day_dir, exchange="binance", top_n=1, include_trades=True),
        emit=frames.append,
    )

    assert [f["type"] for f in frames] == ["book", "trade", "book"]
    assert [f["recv_seq"] for f in frames] == [11, 12, 13]
    assert frames[1]["trade_id"] == 999
    assert frames[1]["price"] == 100.5
    assert frames[1]["qty"] == 0.25
    assert stats.diffs_applied == 2
    assert stats.trades_emitted == 1
    assert stats.frames_emitted == 3


def test_replay_trade_frames_skipped_while_segment_invalid_until_next_snapshot(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[
            {"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 150, "u": 150, "b": [], "a": []},  # gap
            {"recv_ms": 2100, "recv_seq": 21, "E": 2100, "U": 201, "u": 201, "b": [["100.00", "2.00"]], "a": []},
        ],
        events_rows=[
            [1, 1000, 10, 1, "snapshot_loaded", 0, json.dumps({"tag": "initial", "path": "snapshots/snapshot_000001_initial.csv"})],
            [2, 1500, 20, 1, "resync_start", 1, json.dumps({"tag": "resync_000001"})],
            [3, 2000, 20, 1, "snapshot_loaded", 1, json.dumps({"tag": "resync_000001", "path": "snapshots/snapshot_000003_resync_000001.csv"})],
        ],
    )
    _write_snapshot(day_dir, "snapshot_000003_resync_000001.csv", last_update_id=200)
    _write_trades(
        day_dir,
        "BTCUSDT",
        [
            [1150, 1150, 12, 1, 1001, 1150, "100.40", "0.10", 0, "buy", "", "binance", "BTCUSDT"],  # invalid range; dropped
            [2050, 2050, 22, 1, 1002, 2050, "100.60", "0.20", 1, "sell", "", "binance", "BTCUSDT"],  # valid second segment
        ],
    )

    frames: list[dict] = []
    stats = replay_orderbook_day(
        ReplayConfig(day_dir=day_dir, exchange="binance", on_error="best-effort", include_trades=True, top_n=1),
        emit=frames.append,
    )

    assert [f["type"] for f in frames] == ["discontinuity", "book", "trade"]
    assert [f["recv_seq"] for f in frames] == [11, 21, 22]
    assert stats.trades_emitted == 1


def test_replay_include_trades_requires_trade_file(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[{"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 101, "u": 101, "b": [], "a": []}],
    )
    (day_dir / "trades_ws_BTCUSDT_20260221.csv.gz").unlink()

    with pytest.raises(ReplayDataError):
        replay_orderbook_day(ReplayConfig(day_dir=day_dir, exchange="binance", include_trades=True))


def test_replay_supports_schema_v2_and_old_snapshot_path_and_trade_header(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[{"recv_ms": 1100, "recv_seq": 11, "E": 1100, "U": 101, "u": 101, "b": [["100.00", "1.10"]], "a": []}],
    )
    _write_schema_version(day_dir, "BTCUSDT", schema_version=2)
    _write_events(
        day_dir,
        "BTCUSDT",
        [
            [
                1771635606517,
                1000,
                10,
                1,
                "snapshot_loaded",
                0,
                json.dumps(
                    {
                        "tag": "initial",
                        "path": "data/BTCUSDT/20260221/snapshots/snapshot_1771635606517_initial.csv",
                    }
                ),
            ]
        ],
    )
    # Create the actual snapshot file named in the old path convention.
    old_snapshot = day_dir / "snapshots" / "snapshot_1771635606517_initial.csv"
    _write_snapshot(day_dir, old_snapshot.name, last_update_id=100)
    _write_trades_v2(
        day_dir,
        "BTCUSDT",
        [
            [1150, 1150, 12, 1, 999, 1150, "100.50", "0.25", 1],
        ],
    )

    frames: list[dict] = []
    stats = replay_orderbook_day(
        ReplayConfig(day_dir=day_dir, exchange="binance", include_trades=True, top_n=1),
        emit=frames.append,
    )

    assert [f["type"] for f in frames] == ["book", "trade"]
    assert frames[1]["side"] == "sell"
    assert frames[1]["exchange"] == "binance"
    assert frames[1]["symbol"] == "BTCUSDT"
    assert stats.trades_emitted == 1


def test_replay_binance_raises_on_malformed_diff_json(tmp_path: Path) -> None:
    day_dir = _make_binance_day(
        tmp_path,
        diffs=[],
    )
    diff_path = day_dir / "diffs" / "depth_diffs_BTCUSDT_20260221.ndjson.gz"
    with gzip.open(diff_path, "wt", encoding="utf-8") as f:
        f.write("{not json}\n")

    with pytest.raises(ReplayDataError):
        replay_orderbook_day(ReplayConfig(day_dir=day_dir, exchange="binance"))


def test_replay_kraken_uses_raw_snapshot_json_for_checksum_state(tmp_path: Path) -> None:
    day_dir = tmp_path / "data" / "kraken" / "BTCUSDC" / "20260221"
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "schema.json").write_text(
        json.dumps(
            {
                "schema_version": 4,
                "files": {
                    "events_csv": {"path": "events_BTCUSDC_20260221.csv.gz"},
                    "depth_diffs_ndjson_gz": {
                        "path": "diffs/depth_diffs_BTCUSDC_20260221.ndjson.gz",
                        "depth": 25,
                    },
                    "trades_ws_csv": {"path": "trades_ws_BTCUSDC_20260221.csv.gz"},
                },
            }
        ),
        encoding="utf-8",
    )

    snapshot_csv = day_dir / "snapshots" / "snapshot_000001_initial.csv"
    snapshot_csv.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "event_id", "side", "price", "qty", "lastUpdateId", "checksum"])
        w.writerow([1, 1, "bid", "100.10000000", "1.00000000", 0, 0])
        w.writerow([1, 1, "ask", "101.20000000", "2.00000000", 0, 0])

    raw_snapshot = {
        "channel": "book",
        "type": "snapshot",
        "data": {
            "symbol": "BTC/USDC",
            "bids": [{"price": "100.1", "qty": "1.0"}],
            "asks": [{"price": "101.2", "qty": "2.0"}],
        },
    }
    book = KrakenBook(depth=25)
    book.load_snapshot([["100.1", "1.0"]], [["101.2", "2.0"]])
    raw_snapshot["data"]["checksum"] = book.checksum(10)
    raw_json_path = day_dir / "snapshots" / "snapshot_000001_initial.json"
    raw_json_path.write_text(json.dumps(raw_snapshot), encoding="utf-8")

    book.apply_update([], [["101.2", "1.5"]])
    diff_checksum = book.checksum(10)

    _write_events(
        day_dir,
        "BTCUSDC",
        [
            [
                1,
                1000,
                10,
                1,
                "snapshot_loaded",
                0,
                json.dumps(
                    {
                        "tag": "initial",
                        "path": "snapshots/snapshot_000001_initial.csv",
                        "raw_path": "snapshots/snapshot_000001_initial.json",
                        "checksum": raw_snapshot["data"]["checksum"],
                    }
                ),
            ]
        ],
    )
    _write_diffs(
        day_dir,
        "BTCUSDC",
        [
            {
                "recv_ms": 1100,
                "recv_seq": 11,
                "E": 1100,
                "U": 0,
                "u": 0,
                "b": [],
                "a": [["101.2", "1.5"]],
                "checksum": diff_checksum,
                "exchange": "kraken",
                "symbol": "BTC/USDC",
            }
        ],
    )
    _write_trades(day_dir, "BTCUSDC", [])

    frames: list[dict] = []
    stats = replay_orderbook_day(
        ReplayConfig(day_dir=day_dir, exchange="kraken", top_n=1),
        emit=frames.append,
    )

    assert stats.gaps == 0
    assert stats.diffs_applied == 1
    assert [f["type"] for f in frames] == ["book"]
    assert frames[0]["asks"][0] == [101.2, 1.5]
