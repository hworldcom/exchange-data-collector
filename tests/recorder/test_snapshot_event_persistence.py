from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import mm_recorder.recorder_callbacks as callbacks_mod
from mm_recorder.exchanges.types import BookSnapshot
from mm_recorder.recorder_types import RecorderState


class _FakeEmitter:
    def __init__(self, event_id_start: int = 100) -> None:
        self._event_id = int(event_id_start)
        self.events: list[tuple[str, dict | str, int | None]] = []
        self.phase_changes: list[tuple[object, str | None]] = []

    def set_phase(self, new_phase, reason: str | None = None) -> None:
        self.phase_changes.append((new_phase, reason))

    def reserve_event_id(self) -> int:
        self._event_id += 1
        return self._event_id

    def emit_event(self, ev_type: str, details: dict | str, *, event_id: int | None = None) -> int:
        self.events.append((ev_type, details, event_id))
        return int(event_id) if event_id is not None else (self._event_id + len(self.events))


class _FakeEngine:
    def __init__(self) -> None:
        self.adopted: list[BookSnapshot] = []

    def adopt_snapshot(self, snapshot: BookSnapshot) -> None:
        self.adopted.append(snapshot)


def _make_ctx(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        snapshots_dir=tmp_path / "snapshots",
        run_id=1,
        engine=_FakeEngine(),
        state=RecorderState(),
    )


def test_handle_snapshot_emits_snapshot_loaded_only_after_persistence(tmp_path: Path) -> None:
    ctx = _make_ctx(tmp_path)
    emitter = _FakeEmitter(event_id_start=123)
    heartbeat = SimpleNamespace(stream=None)
    snapshotter = callbacks_mod.RecorderSnapshotter(ctx, emitter, heartbeat)

    snap = BookSnapshot(
        event_time_ms=0,
        bids=[["100.0", "1.0"]],
        asks=[["101.0", "2.0"]],
        checksum=7,
        raw={"source": "test"},
    )
    snapshotter.handle_snapshot(snap, "initial")

    snapshot_path = ctx.snapshots_dir / "snapshot_000124_initial.csv"
    raw_path = ctx.snapshots_dir / "snapshot_000124_initial.json"
    assert snapshot_path.exists()
    assert raw_path.exists()

    event_types = [ev[0] for ev in emitter.events]
    assert "snapshot_loaded" in event_types
    loaded_details = [ev[1] for ev in emitter.events if ev[0] == "snapshot_loaded"][0]
    assert isinstance(loaded_details, dict)
    assert loaded_details["path"] == str(snapshot_path)
    assert loaded_details["raw_path"] == str(raw_path)
    assert loaded_details["checksum"] == 7
    assert len(ctx.engine.adopted) == 1


def test_handle_snapshot_write_failure_does_not_emit_snapshot_loaded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_ctx(tmp_path)
    emitter = _FakeEmitter(event_id_start=10)
    heartbeat = SimpleNamespace(stream=None)
    snapshotter = callbacks_mod.RecorderSnapshotter(ctx, emitter, heartbeat)

    def _boom(**_kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(callbacks_mod, "write_snapshot_csv", _boom)

    snap = BookSnapshot(
        event_time_ms=0,
        bids=[["100.0", "1.0"]],
        asks=[["101.0", "2.0"]],
        checksum=9,
        raw=None,
    )

    with pytest.raises(RuntimeError, match="disk full"):
        snapshotter.handle_snapshot(snap, "initial")

    event_types = [ev[0] for ev in emitter.events]
    assert "snapshot_write_failed" in event_types
    assert "snapshot_loaded" not in event_types
    assert len(ctx.engine.adopted) == 0
    assert not (ctx.snapshots_dir / "snapshot_000011_initial.csv").exists()
