import csv
import gzip
import json
import os

import pytest
import mm_recorder.recorder as recorder_mod
from mm_recorder.recv_seq_checkpoint import (
    FileFingerprint,
    advance,
    matches_current_files,
    write_atomic,
)


def _write_authority_files(day_dir):
    day_dir.mkdir(parents=True, exist_ok=True)

    events_path = day_dir / "events_BTCUSDT_20251215.csv.gz"
    with gzip.open(events_path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"])
        writer.writerow([1, 1000, 10, 111, "run_start", 0, "{}"])

    gaps_path = day_dir / "gaps_BTCUSDT_20251215.csv.gz"
    with gzip.open(gaps_path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"])
        writer.writerow([1001, 17, 111, 0, "resync_start", "gap"])

    trades_path = day_dir / "trades_ws_BTCUSDT_20251215.csv.gz"
    with gzip.open(trades_path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
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
        writer.writerow([1002, 1002, 24, 111, 1, 1002, "100.0", "0.1", 0, "buy", "", "binance", "BTCUSDT"])

    diff_dir = day_dir / "diffs"
    diff_dir.mkdir(parents=True, exist_ok=True)
    diff_path = diff_dir / "depth_diffs_BTCUSDT_20251215.ndjson.gz"
    with gzip.open(diff_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"recv_ms": 1003, "recv_seq": 31, "E": 1003, "U": 10, "u": 11, "b": [], "a": []}) + "\n")

    authority_paths = recorder_mod._build_recv_seq_authority(
        day_dir=day_dir,
        events_path=events_path,
        gaps_path=gaps_path,
        trades_path=trades_path,
        diff_path=diff_path,
    )
    return authority_paths


def _write_valid_checkpoint(day_dir, authority_paths, last_recv_seq=31):
    current_files = recorder_mod._stat_recv_seq_authority(day_dir, authority_paths)
    checkpoint = advance(None, last_recv_seq, current_files)
    checkpoint_path = day_dir / "state" / "recv_seq.json"
    write_atomic(checkpoint_path, checkpoint)
    return checkpoint_path, checkpoint


def _startup_restore_seed(day_dir, authority_paths):
    checkpoint_path = day_dir / "state" / "recv_seq.json"
    try:
        seed, _ = recorder_mod._restore_recv_seq_seed_from_checkpoint(
            checkpoint_path=checkpoint_path,
            day_dir=day_dir,
            authority_paths=authority_paths,
        )
    except Exception:
        seed = None
    if seed is None:
        return recorder_mod._restore_recv_seq_seed(
            events_path=authority_paths["events"],
            gaps_path=authority_paths["gaps"],
            trades_path=authority_paths["trades_ws"],
            diff_path=authority_paths.get("depth_diffs"),
        )
    return seed


def test_checkpoint_inventory_name_mismatch_forces_fallback(tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    current_files = recorder_mod._stat_recv_seq_authority(day_dir, authority_paths)
    checkpoint_path = day_dir / "state" / "recv_seq.json"
    mismatched_files = {
        "events": current_files["events"],
        "gaps": current_files["gaps"],
        "trades": FileFingerprint(
            path=current_files["trades_ws"].path,
            size=current_files["trades_ws"].size,
            mtime_ns=current_files["trades_ws"].mtime_ns,
        ),
    }
    checkpoint = advance(None, 17, mismatched_files)
    write_atomic(checkpoint_path, checkpoint)

    seed, _ = recorder_mod._restore_recv_seq_seed_from_checkpoint(
        checkpoint_path=checkpoint_path,
        day_dir=day_dir,
        authority_paths=authority_paths,
    )

    assert seed is None
    assert not matches_current_files(checkpoint, current_files)


def test_checkpoint_never_moves_backward():
    current_files = {
        "events": FileFingerprint(path="events.csv.gz", size=100, mtime_ns=1),
    }
    checkpoint = advance(None, 20, current_files)
    checkpoint = advance(checkpoint, 7, current_files)
    assert checkpoint.last_recv_seq == 20


def test_corrupt_checkpoint_falls_back_to_scan(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    checkpoint_path = day_dir / "state" / "recv_seq.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text("{not-json", encoding="utf-8")

    scan_calls = {"n": 0}
    original_scan = recorder_mod._restore_recv_seq_seed

    def tracked_scan(**kwargs):
        scan_calls["n"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(recorder_mod, "_restore_recv_seq_seed", tracked_scan)

    seed = _startup_restore_seed(day_dir, authority_paths)

    assert scan_calls["n"] == 1
    assert seed == 31


def test_unsupported_schema_version_falls_back_to_scan(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    checkpoint_path, checkpoint = _write_valid_checkpoint(day_dir, authority_paths)
    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    payload["schema_version"] = checkpoint.schema_version + 1
    checkpoint_path.write_text(json.dumps(payload), encoding="utf-8")

    scan_calls = {"n": 0}
    original_scan = recorder_mod._restore_recv_seq_seed

    def tracked_scan(**kwargs):
        scan_calls["n"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(recorder_mod, "_restore_recv_seq_seed", tracked_scan)

    seed = _startup_restore_seed(day_dir, authority_paths)

    assert scan_calls["n"] == 1
    assert seed == 31


def test_missing_tracked_file_falls_back_to_scan(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    _write_valid_checkpoint(day_dir, authority_paths)
    authority_paths["trades_ws"].unlink()

    scan_calls = {"n": 0}
    original_scan = recorder_mod._restore_recv_seq_seed

    def tracked_scan(**kwargs):
        scan_calls["n"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(recorder_mod, "_restore_recv_seq_seed", tracked_scan)

    seed = _startup_restore_seed(day_dir, authority_paths)

    assert scan_calls["n"] == 1
    assert seed == 31


def test_size_change_falls_back_to_scan(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    _write_valid_checkpoint(day_dir, authority_paths)
    with gzip.open(authority_paths["trades_ws"], "at", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([1004, 1004, 33, 111, 2, 1004, "101.0", "0.2", 1, "sell", "", "binance", "BTCUSDT"])

    scan_calls = {"n": 0}
    original_scan = recorder_mod._restore_recv_seq_seed

    def tracked_scan(**kwargs):
        scan_calls["n"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(recorder_mod, "_restore_recv_seq_seed", tracked_scan)

    seed = _startup_restore_seed(day_dir, authority_paths)

    assert scan_calls["n"] == 1
    assert seed == 33


def test_mtime_change_falls_back_to_scan(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    _write_valid_checkpoint(day_dir, authority_paths)
    stat_result = authority_paths["events"].stat()
    os.utime(
        authority_paths["events"],
        ns=(stat_result.st_atime_ns, stat_result.st_mtime_ns + 1_000_000_000),
    )

    scan_calls = {"n": 0}
    original_scan = recorder_mod._restore_recv_seq_seed

    def tracked_scan(**kwargs):
        scan_calls["n"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(recorder_mod, "_restore_recv_seq_seed", tracked_scan)

    seed = _startup_restore_seed(day_dir, authority_paths)

    assert scan_calls["n"] == 1
    assert seed == 31


def test_stale_checkpoint_after_file_advance_recovers_via_scan(monkeypatch, tmp_path):
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20251215"
    authority_paths = _write_authority_files(day_dir)
    _write_valid_checkpoint(day_dir, authority_paths, last_recv_seq=31)
    with gzip.open(authority_paths["trades_ws"], "at", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([1005, 1005, 45, 111, 3, 1005, "102.0", "0.3", 0, "buy", "", "binance", "BTCUSDT"])

    scan_calls = {"n": 0}
    original_scan = recorder_mod._restore_recv_seq_seed

    def tracked_scan(**kwargs):
        scan_calls["n"] += 1
        return original_scan(**kwargs)

    monkeypatch.setattr(recorder_mod, "_restore_recv_seq_seed", tracked_scan)

    seed = _startup_restore_seed(day_dir, authority_paths)

    assert scan_calls["n"] == 1
    assert seed == 45
