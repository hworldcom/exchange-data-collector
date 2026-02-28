from __future__ import annotations

from types import SimpleNamespace

import mm_recorder.recorder_callbacks as callbacks_mod
from mm_recorder.recorder_types import RecorderState


class _FakeFile:
    def __init__(self) -> None:
        self.closed = False
        self.flush_count = 0

    def flush(self) -> None:
        self.flush_count += 1

    def close(self) -> None:
        self.closed = True


class _FakeWriter:
    def __init__(self) -> None:
        self.rows = []

    def writerow(self, row) -> None:
        self.rows.append(row)


def _make_emitter() -> tuple[callbacks_mod.RecorderEmitter, _FakeFile, _FakeFile]:
    ev_f = _FakeFile()
    gap_f = _FakeFile()
    ctx = SimpleNamespace(
        ev_f=ev_f,
        gap_f=gap_f,
        ev_w=_FakeWriter(),
        gap_w=_FakeWriter(),
        run_id=1,
        state=RecorderState(),
    )
    return callbacks_mod.RecorderEmitter(ctx), ev_f, gap_f


def test_ledger_flush_uses_batch_and_flush_all(monkeypatch) -> None:
    monkeypatch.setattr(callbacks_mod, "EVENTS_FLUSH_ROWS", 3)
    monkeypatch.setattr(callbacks_mod, "GAPS_FLUSH_ROWS", 2)
    monkeypatch.setattr(callbacks_mod, "LEDGER_FLUSH_INTERVAL_SEC", 9999.0)
    emitter, ev_f, gap_f = _make_emitter()

    emitter.emit_event("a", {})
    emitter.emit_event("b", {})
    assert ev_f.flush_count == 0
    emitter.emit_event("c", {})
    assert ev_f.flush_count == 1

    emitter.write_gap("g1", "x")
    assert gap_f.flush_count == 0
    emitter.write_gap("g2", "x")
    assert gap_f.flush_count == 1

    emitter.emit_event("d", {})
    emitter.write_gap("g3", "x")
    assert ev_f.flush_count == 1
    assert gap_f.flush_count == 1
    emitter.flush_all()
    assert ev_f.flush_count == 2
    assert gap_f.flush_count == 2
