from __future__ import annotations

from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import OrderBookSyncEngine


def test_bridge_condition_uses_last_update_id_plus_one():
    lob = LocalOrderBook()
    lob.load_snapshot(bids=[["100", "1"]], asks=[["101", "1"]], last_update_id=100)
    engine = OrderBookSyncEngine(lob)
    engine.snapshot_loaded = True

    engine.buffer = [
        {"U": 101, "u": 105, "b": [], "a": []},  # bridge: lastUpdateId+1 = 101
    ]

    result = engine.feed_depth_event({"U": 106, "u": 106, "b": [], "a": []})
    assert engine.depth_synced is True
    assert engine.lob.last_update_id == 106
    assert result.action in ("synced", "applied")
    assert engine.buffer == []
