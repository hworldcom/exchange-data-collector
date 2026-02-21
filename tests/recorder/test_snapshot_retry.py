from __future__ import annotations

from pathlib import Path

import mm_recorder.snapshot as snapshot_mod


def test_record_rest_snapshot_retries(monkeypatch, tmp_path: Path):
    calls = {"n": 0}

    class FlakyClient:
        def get_order_book(self, symbol: str, limit: int):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("temporary failure")
            return {
                "lastUpdateId": 10,
                "bids": [["100", "1"]],
                "asks": [["101", "1"]],
            }

    monkeypatch.setattr(snapshot_mod, "SNAPSHOT_RETRY_MAX", 3)
    monkeypatch.setattr(snapshot_mod, "SNAPSHOT_RETRY_BACKOFF_S", 0.0)
    monkeypatch.setattr(snapshot_mod, "SNAPSHOT_RETRY_BACKOFF_MAX_S", 0.0)
    monkeypatch.setattr(snapshot_mod.time, "sleep", lambda s: None)

    lob, path, last_uid, snap = snapshot_mod.record_rest_snapshot(
        client=FlakyClient(),
        symbol="BTCUSDT",
        day_dir=tmp_path,
        snapshots_dir=tmp_path,
        limit=5,
        run_id=1,
        event_id=1,
        tag="test",
        decimals=2,
    )

    assert calls["n"] == 3
    assert last_uid == 10
    assert path.exists()
    assert snap["lastUpdateId"] == 10
