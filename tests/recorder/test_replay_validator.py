import csv
import gzip
import json
from pathlib import Path

import mm_recorder.replay_validator as rv


def _write_schema(day_dir: Path, events_name: str) -> None:
    schema = {
        "schema_version": 3,
        "files": {
            "events_csv": {"path": events_name},
        },
    }
    (day_dir / "schema.json").write_text(json.dumps(schema), encoding="utf-8")


def _write_events(path: Path, details: dict) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"])
        w.writerow([1, 1, 10, 1, "snapshot_loaded", 0, json.dumps(details)])


def _write_snapshot(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "event_id", "side", "price", "qty", "lastUpdateId"])
        w.writerow([1, 1, "bid", "100.0", "1.0", 10])
        w.writerow([1, 1, "ask", "101.0", "1.0", 10])


def _write_diffs(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def test_replay_validator_binance_ok(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20250101"
    day_dir.mkdir(parents=True, exist_ok=True)

    events_name = "events_BTCUSDT_20250101.csv.gz"
    events_path = day_dir / events_name
    snapshot_path = day_dir / "snapshots" / "snapshot_000001_initial.csv"
    diffs_path = day_dir / "diffs" / "depth_diffs_BTCUSDT_20250101.ndjson.gz"

    _write_schema(day_dir, events_name)
    _write_snapshot(snapshot_path)
    _write_events(events_path, {"tag": "initial", "path": str(snapshot_path.relative_to(day_dir))})
    _write_diffs(
        diffs_path,
        [
            {"recv_seq": 11, "E": 1, "U": 10, "u": 11, "b": [], "a": []},
            {"recv_seq": 12, "E": 2, "U": 12, "u": 12, "b": [], "a": []},
        ],
    )

    import sys
    monkeypatch.setattr(sys, "argv", ["replay_validator", "--day-dir", str(day_dir)])
    assert rv.main() == 0


def test_replay_validator_binance_gap(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20250102"
    day_dir.mkdir(parents=True, exist_ok=True)

    events_name = "events_BTCUSDT_20250102.csv.gz"
    events_path = day_dir / events_name
    snapshot_path = day_dir / "snapshots" / "snapshot_000001_initial.csv"
    diffs_path = day_dir / "diffs" / "depth_diffs_BTCUSDT_20250102.ndjson.gz"

    _write_schema(day_dir, events_name)
    _write_snapshot(snapshot_path)
    _write_events(events_path, {"tag": "initial", "path": str(snapshot_path.relative_to(day_dir))})
    _write_diffs(
        diffs_path,
        [
            {"recv_seq": 11, "E": 1, "U": 50, "u": 51, "b": [], "a": []},
        ],
    )

    import sys
    monkeypatch.setattr(sys, "argv", ["replay_validator", "--day-dir", str(day_dir)])
    assert rv.main() == 1
