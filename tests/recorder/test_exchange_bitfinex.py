from mm_recorder.exchanges.bitfinex import BitfinexAdapter


def test_bitfinex_adapter_book_snapshot_update_checksum():
    adapter = BitfinexAdapter()
    adapter.book_chan_id = 100

    # Snapshot
    snap_msg = [100, [[100.0, 1, 0.5], [101.0, 2, -0.4]]]
    snaps, diffs, trades = adapter.parse_ws_message(snap_msg)
    assert len(snaps) == 1
    assert len(diffs) == 0
    assert len(trades) == 0
    assert snaps[0].bids[0][0] == "100.0"
    assert snaps[0].asks[0][0] == "101.0"

    # Update
    upd_msg = [100, 100.0, 1, 0.7]
    snaps, diffs, trades = adapter.parse_ws_message(upd_msg)
    assert len(snaps) == 0
    assert len(diffs) == 1
    assert diffs[0].raw["type"] == "update"
    assert diffs[0].bids[0][0] == "100.0"

    # Checksum frame
    cs_msg = [100, "cs", 123]
    snaps, diffs, trades = adapter.parse_ws_message(cs_msg)
    assert len(diffs) == 1
    assert diffs[0].checksum == 123


def test_bitfinex_adapter_trades():
    adapter = BitfinexAdapter()
    adapter.trades_chan_id = 200

    # Snapshot trades
    snap_msg = [200, [[1, 1000, 1.0, 0.5], [2, 1001, -0.2, 1.1]]]
    snaps, diffs, trades = adapter.parse_ws_message(snap_msg)
    assert len(trades) == 2

    # Update trade (tu)
    upd_msg = [200, "tu", 3, 1700000000000, -0.1, 1.2]
    snaps, diffs, trades = adapter.parse_ws_message(upd_msg)
    assert len(trades) == 1
    assert trades[0].is_buyer_maker == 1
    assert trades[0].side == "sell"
