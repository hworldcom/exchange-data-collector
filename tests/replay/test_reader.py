from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

import pytest

from mm_replay.reader import ReplayDataError, build_segments, read_events


def _write_events_csv(path: Path, rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"])
        w.writerows(rows)


def test_build_segments_splits_on_resync_start(tmp_path: Path) -> None:
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260221"
    events_path = day_dir / "events_BTCUSDT_20260221.csv.gz"
    _write_events_csv(
        events_path,
        [
            [1, 1000, 10, 1, "snapshot_loaded", 0, json.dumps({"tag": "initial"})],
            [2, 1001, 20, 1, "resync_start", 1, json.dumps({"tag": "resync_000001"})],
            [3, 1002, 30, 1, "snapshot_loaded", 1, json.dumps({"tag": "resync_000001"})],
            [4, 1003, 40, 1, "resync_start", 2, json.dumps({"tag": "resync_000002"})],
            [5, 1004, 50, 1, "snapshot_loaded", 2, json.dumps({"tag": "resync_000002"})],
        ],
    )

    events = read_events(events_path)
    segments = build_segments(day_dir, events)

    assert [s.recv_seq for s in segments] == [10, 30, 50]
    assert [s.end_recv_seq for s in segments] == [20, 40, None]
    assert segments[0].snapshot_path == day_dir / "snapshots" / "snapshot_000001_initial.csv"
    assert segments[1].snapshot_path == day_dir / "snapshots" / "snapshot_000003_resync_000001.csv"


def test_read_events_raises_on_invalid_details_json(tmp_path: Path) -> None:
    events_path = tmp_path / "events.csv.gz"
    _write_events_csv(
        events_path,
        [
            [1, 1000, 10, 1, "snapshot_loaded", 0, "{bad json"],
        ],
    )

    with pytest.raises(ReplayDataError):
        read_events(events_path)


def test_build_segments_resolves_project_relative_data_path(tmp_path: Path) -> None:
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260221"
    events_path = day_dir / "events_BTCUSDT_20260221.csv.gz"
    snapshot_path = day_dir / "snapshots" / "snapshot_000001_initial.csv"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("dummy\n", encoding="utf-8")

    _write_events_csv(
        events_path,
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
                        "path": "data/binance/BTCUSDT/20260221/snapshots/snapshot_000001_initial.csv",
                    }
                ),
            ],
        ],
    )

    events = read_events(events_path)
    segments = build_segments(day_dir, events)

    assert len(segments) == 1
    assert segments[0].snapshot_path == snapshot_path


def test_build_segments_splits_on_next_snapshot_loaded_without_resync_start(tmp_path: Path) -> None:
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260221"
    events_path = day_dir / "events_BTCUSDT_20260221.csv.gz"
    _write_events_csv(
        events_path,
        [
            [1, 1000, 10, 1, "snapshot_loaded", 0, json.dumps({"tag": "initial"})],
            [2, 1001, 30, 1, "snapshot_loaded", 0, json.dumps({"tag": "manual_refresh"})],
            [3, 1002, 40, 1, "resync_start", 1, json.dumps({"tag": "resync_000001"})],
            [4, 1003, 50, 1, "snapshot_loaded", 1, json.dumps({"tag": "resync_000001"})],
        ],
    )

    events = read_events(events_path)
    segments = build_segments(day_dir, events)

    assert [s.recv_seq for s in segments] == [10, 30, 50]
    assert [s.end_recv_seq for s in segments] == [30, 40, None]


def test_build_segments_resolves_raw_snapshot_json_path(tmp_path: Path) -> None:
    day_dir = tmp_path / "data" / "kraken" / "BTCUSDC" / "20260222"
    events_path = day_dir / "events_BTCUSDC_20260222.csv.gz"
    snapshot_path = day_dir / "snapshots" / "snapshot_000001_initial.csv"
    raw_snapshot_path = day_dir / "snapshots" / "snapshot_000001_initial.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("dummy\n", encoding="utf-8")
    raw_snapshot_path.write_text("{}", encoding="utf-8")

    _write_events_csv(
        events_path,
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
                    }
                ),
            ],
        ],
    )

    events = read_events(events_path)
    segments = build_segments(day_dir, events)

    assert len(segments) == 1
    assert segments[0].snapshot_path == snapshot_path
    assert segments[0].raw_snapshot_path == raw_snapshot_path
