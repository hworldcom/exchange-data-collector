from __future__ import annotations

import pytest

from mm_core.local_orderbook import LocalOrderBook


def test_tick_alignment_tolerates_tiny_rounding_error():
    lob = LocalOrderBook(tick_size="0.01")
    lob.load_snapshot(bids=[["1.0000000000000002", "1"]], asks=[], last_update_id=1)
    bids, _ = lob.top_n(1)
    assert bids[0][0] == pytest.approx(1.0)


def test_tick_alignment_rejects_large_mismatch():
    lob = LocalOrderBook(tick_size="0.01")
    with pytest.raises(ValueError, match="tick_size"):
        lob.load_snapshot(bids=[["1.005", "1"]], asks=[], last_update_id=1)
