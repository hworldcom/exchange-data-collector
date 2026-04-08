from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from mm_recorder.live_writer import LiveNdjsonWriter


def test_live_writer_rotates_and_retains(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_700_000_000.0

    def _time() -> float:
        return now

    monkeypatch.setattr("mm_recorder.live_writer.time.time", _time)

    path = tmp_path / "live_depth_diffs.ndjson"
    writer = LiveNdjsonWriter(path, rotate_interval_s=10, retention_s=30)

    writer.write_line(json.dumps({"seq": 1}) + "\n")
    assert path.exists()

    # Force rotation by advancing time
    now += 11
    writer.write_line(json.dumps({"seq": 2}) + "\n")

    rotated = path.with_name(f"{path.name}.0")
    assert rotated.exists()

    # Make rotated file old enough for retention cleanup
    old_mtime = time.time() - 60
    os.utime(rotated, (old_mtime, old_mtime))

    # Next rotation should trigger cleanup
    now += 11
    writer.write_line(json.dumps({"seq": 3}) + "\n")

    assert not rotated.exists()
    writer.close()


def test_live_writer_restart_keeps_existing_rotated_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_700_000_000.0

    def _time() -> float:
        return now

    monkeypatch.setattr("mm_recorder.live_writer.time.time", _time)

    path = tmp_path / "live_depth_diffs.ndjson"

    first = LiveNdjsonWriter(path, rotate_interval_s=10, retention_s=300)
    first.write_line(json.dumps({"seq": 1}) + "\n")
    now += 11
    first.write_line(json.dumps({"seq": 2}) + "\n")
    first.close()

    rotated0 = path.with_name(f"{path.name}.0")
    assert rotated0.exists()
    original_rotated0 = rotated0.read_text(encoding="utf-8")

    second = LiveNdjsonWriter(path, rotate_interval_s=10, retention_s=300)
    second.write_line(json.dumps({"seq": 3}) + "\n")
    now += 11
    second.write_line(json.dumps({"seq": 4}) + "\n")
    second.close()

    rotated1 = path.with_name(f"{path.name}.1")
    assert rotated0.read_text(encoding="utf-8") == original_rotated0
    assert rotated1.exists()
    assert '"seq": 1' in rotated0.read_text(encoding="utf-8")
    assert '"seq": 2' in rotated1.read_text(encoding="utf-8")
    assert '"seq": 3' in rotated1.read_text(encoding="utf-8")
