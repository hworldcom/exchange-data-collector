from mm_core.checksum.kraken import KrakenSyncEngine
from mm_core.checksum.base import BookSnapshot, DepthDiff


def test_kraken_checksum_engine_gap():
    engine = KrakenSyncEngine(depth=25)

    snapshot = BookSnapshot(
        event_time_ms=0,
        bids=[["100.0", "1.0"]],
        asks=[["101.0", "2.0"]],
        checksum=None,
    )
    engine.adopt_snapshot(snapshot)

    diff = DepthDiff(
        event_time_ms=0,
        U=0,
        u=0,
        bids=[["100.0", "1.5"]],
        asks=[],
        checksum=123,
    )
    result = engine.feed_depth_event(diff)
    assert result.action == "gap"
