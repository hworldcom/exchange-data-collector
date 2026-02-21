from __future__ import annotations

import importlib
import os


def test_invalid_env_does_not_crash(monkeypatch):
    monkeypatch.setenv("WS_PING_INTERVAL_S", "not-a-number")
    monkeypatch.setenv("WS_RECONNECT_BACKOFF_S", "nope")
    monkeypatch.setenv("LIVE_STREAM_ROTATE_S", "bad")
    monkeypatch.setenv("SNAPSHOT_TIMEOUT_S", "invalid")
    monkeypatch.setenv("SNAPSHOT_RETRY_MAX", "invalid")

    import mm_recorder.recorder as recorder_mod
    import mm_recorder.snapshot as snapshot_mod

    importlib.reload(recorder_mod)
    importlib.reload(snapshot_mod)

    assert recorder_mod.WS_PING_INTERVAL_S == 20
    assert recorder_mod.WS_RECONNECT_BACKOFF_S == 1.0
    assert recorder_mod.LIVE_STREAM_ROTATE_S == 60.0

    assert snapshot_mod.SNAPSHOT_TIMEOUT_S == 10.0
    assert snapshot_mod.SNAPSHOT_RETRY_MAX == 3
