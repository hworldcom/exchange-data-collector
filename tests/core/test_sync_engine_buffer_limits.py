from __future__ import annotations

from mm_core.sync_engine import OrderBookSyncEngine
from mm_core.checksum.kraken import KrakenSyncEngine
from mm_core.checksum.bitfinex import BitfinexSyncEngine
from mm_core.checksum.base import DepthDiff


def test_orderbook_sync_engine_buffer_overflow():
    engine = OrderBookSyncEngine(max_buffer_size=2)
    result1 = engine.feed_depth_event({"U": 1, "u": 1, "b": [], "a": []})
    result2 = engine.feed_depth_event({"U": 2, "u": 2, "b": [], "a": []})
    result3 = engine.feed_depth_event({"U": 3, "u": 3, "b": [], "a": []})

    assert result1.action == "buffered"
    assert result2.action == "buffered"
    assert result3.action == "gap"
    assert result3.details == "buffer_overflow"
    assert engine.buffer == []


def test_kraken_sync_engine_buffer_overflow():
    engine = KrakenSyncEngine(depth=10, max_buffer_size=2)
    diff = DepthDiff(event_time_ms=0, U=0, u=0, bids=[], asks=[], checksum=None, raw=None)

    r1 = engine.feed_depth_event(diff)
    r2 = engine.feed_depth_event(diff)
    r3 = engine.feed_depth_event(diff)

    assert r1.action == "buffered"
    assert r2.action == "buffered"
    assert r3.action == "gap"
    assert r3.details == "buffer_overflow"
    assert engine.buffer == []


def test_bitfinex_sync_engine_buffer_overflow():
    engine = BitfinexSyncEngine(depth=25, max_buffer_size=2)
    diff = DepthDiff(event_time_ms=0, U=0, u=0, bids=[], asks=[], checksum=None, raw=None)

    r1 = engine.feed_depth_event(diff)
    r2 = engine.feed_depth_event(diff)
    r3 = engine.feed_depth_event(diff)

    assert r1.action == "buffered"
    assert r2.action == "buffered"
    assert r3.action == "gap"
    assert r3.details == "buffer_overflow"
    assert engine.buffer == []
