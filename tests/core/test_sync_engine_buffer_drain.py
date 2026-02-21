from __future__ import annotations

from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import OrderBookSyncEngine


def test_sync_engine_drain_buffer_keeps_only_unbridged_events():
    lob = LocalOrderBook()
    lob.load_snapshot(bids=[["100", "1"]], asks=[["101", "1"]], last_update_id=100)
    engine = OrderBookSyncEngine(lob)
    engine.snapshot_loaded = True

    engine.buffer = [
        {"U": 90, "u": 95, "b": [], "a": []},     # stale
        {"U": 120, "u": 125, "b": [], "a": []},   # unbridged, should remain
    ]

    result = engine.feed_depth_event({"U": 130, "u": 135, "b": [], "a": []})
    assert result.action == "buffered"

    remaining = {(int(ev["U"]), int(ev["u"])) for ev in engine.buffer}
    assert remaining == {(120, 125), (130, 135)}


def test_sync_engine_bridge_applies_buffer_in_order():
    lob = LocalOrderBook()
    lob.load_snapshot(bids=[["100", "1"]], asks=[["101", "1"]], last_update_id=100)
    engine = OrderBookSyncEngine(lob)
    engine.snapshot_loaded = True

    engine.buffer = [
        {"U": 90, "u": 105, "b": [], "a": []},    # bridge
        {"U": 106, "u": 110, "b": [], "a": []},   # apply after bridge
    ]

    result = engine.feed_depth_event({"U": 111, "u": 111, "b": [], "a": []})
    assert result.action in ("synced", "applied")
    assert engine.depth_synced is True
    assert engine.lob.last_update_id == 111
    assert engine.buffer == []
