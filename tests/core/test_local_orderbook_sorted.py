from __future__ import annotations

from mm_core.local_orderbook import LocalOrderBook


def test_local_orderbook_top_n_sorted():
    lob = LocalOrderBook()
    lob.load_snapshot(
        bids=[["101", "1"], ["100", "2"]],
        asks=[["103", "1"], ["102", "1"]],
        last_update_id=10,
    )

    bids, asks = lob.top_n(1)
    assert bids[0][0] == 101.0
    assert asks[0][0] == 102.0

    # Apply updates: remove 101 bid, add 105 bid, remove 102 ask, add 101.5 ask
    lob.apply_diff(
        U=11,
        u=11,
        bids=[["105", "1"], ["101", "0"]],
        asks=[["102", "0"], ["101.5", "2"]],
    )

    bids, asks = lob.top_n(2)
    assert bids[0][0] == 105.0
    assert bids[1][0] == 100.0
    assert asks[0][0] == 101.5
    assert asks[1][0] == 103.0
