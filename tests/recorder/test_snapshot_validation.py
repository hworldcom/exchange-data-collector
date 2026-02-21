from __future__ import annotations

import pytest

import mm_recorder.snapshot as snapshot_mod


def test_record_rest_snapshot_rejects_invalid_payload(tmp_path):
    class BadClient:
        def get_order_book(self, symbol: str, limit: int):
            return {"bids": [], "asks": []}  # missing lastUpdateId

    with pytest.raises(RuntimeError, match="Invalid snapshot payload"):
        snapshot_mod.record_rest_snapshot(
            client=BadClient(),
            symbol="BTCUSDT",
            day_dir=tmp_path,
            snapshots_dir=tmp_path,
            limit=5,
            run_id=1,
            event_id=1,
            tag="test",
            decimals=2,
        )
