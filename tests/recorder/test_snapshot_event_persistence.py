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
        self.gaps: list[tuple[str, str]] = []

    def set_phase(self, new_phase, reason: str | None = None) -> None:
        self.phase_changes.append((new_phase, reason))

    def reserve_event_id(self) -> int:
        self._event_id += 1
        return self._event_id

    def emit_event(self, ev_type: str, details: dict | str, *, event_id: int | None = None) -> int:
        self.events.append((ev_type, details, event_id))
        return int(event_id) if event_id is not None else (self._event_id + len(self.events))

    def write_gap(self, event: str, details: str) -> None:
        self.gaps.append((event, details))


class _FakeEngine:
    def __init__(self) -> None:
        self.adopted: list[BookSnapshot] = []

    def adopt_snapshot(self, snapshot: BookSnapshot) -> None:
        self.adopted.append(snapshot)


def _make_ctx(tmp_path: Path) -> SimpleNamespace:
    day_dir = tmp_path / "data" / "kraken" / "BTCUSD" / "20260221"
    return SimpleNamespace(
        day_dir=day_dir,
        snapshots_dir=day_dir / "snapshots",
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
    assert loaded_details["path"] == "snapshots/snapshot_000124_initial.csv"
    assert loaded_details["raw_path"] == "snapshots/snapshot_000124_initial.json"
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


def test_resync_checksum_payload_saved_path_is_day_relative(tmp_path: Path) -> None:
    day_dir = tmp_path / "data" / "bitfinex" / "BTCUSD" / "20260221"
    day_dir.mkdir(parents=True, exist_ok=True)
    ctx = SimpleNamespace(
        day_dir=day_dir,
        adapter=SimpleNamespace(sync_mode="checksum"),
        engine=SimpleNamespace(
            last_checksum_payload="b0:1:a0:-1",
            reset_for_resync=lambda: None,
        ),
        state=RecorderState(),
        log=SimpleNamespace(warning=lambda *args, **kwargs: None, exception=lambda *args, **kwargs: None),
    )
    emitter = _FakeEmitter(event_id_start=10)
    stream = SimpleNamespace(disconnect=lambda: None)
    heartbeat = SimpleNamespace(stream=stream)
    snapshotter = callbacks_mod.RecorderSnapshotter(ctx, emitter, heartbeat)

    snapshotter.resync("checksum_mismatch expected=1 got=2")

    event_rows = [(typ, details) for typ, details, _ in emitter.events]
    saved = [details for typ, details in event_rows if typ == "checksum_payload_saved"]
    assert len(saved) == 1
    assert isinstance(saved[0], dict)
    assert saved[0]["tag"] == "resync_000001"
    assert saved[0]["path"] == "debug/checksum_payload_resync_000001.txt"
    assert (day_dir / "debug" / "checksum_payload_resync_000001.txt").read_text(encoding="utf-8") == "b0:1:a0:-1"
