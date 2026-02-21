from mm_core.checksum.bitfinex import BitfinexSyncEngine
from mm_core.checksum.base import BookSnapshot, DepthDiff


def test_bitfinex_checksum_mismatch_triggers_gap():
    engine = BitfinexSyncEngine(depth=25)

    snapshot = BookSnapshot(
        event_time_ms=0,
        bids=[["100.0", "0.5"]],
        asks=[["101.0", "-0.4"]],
        checksum=None,
    )
    engine.adopt_snapshot(snapshot)

    # Apply one update (valid)
    diff = DepthDiff(
        event_time_ms=0,
        U=0,
        u=0,
        bids=[["100.0", "0.7"]],
        asks=[],
        checksum=None,
        raw={"type": "update", "price": "100.0", "count": 1, "amount": "0.7"},
    )
    res = engine.feed_depth_event(diff)
    assert res.action == "applied"

    # Send checksum that doesn't match current book.
    bad_checksum = 123
    diff_cs = DepthDiff(
        event_time_ms=0,
        U=0,
        u=0,
        bids=[],
        asks=[],
        checksum=bad_checksum,
        raw={"type": "checksum", "checksum": bad_checksum},
    )
    res = engine.feed_depth_event(diff_cs)
    assert res.action == "gap"
