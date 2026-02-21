from mm_recorder.exchanges.binance import BinanceAdapter


def test_binance_adapter_parse_depth_and_trade():
    adapter = BinanceAdapter()

    depth = adapter.parse_depth({"E": 123, "U": 10, "u": 11, "b": [["100", "1"]], "a": [["101", "2"]]})
    assert depth.event_time_ms == 123
    assert depth.U == 10 and depth.u == 11
    assert depth.bids[0][0] == "100"
    assert depth.asks[0][0] == "101"

    trade = adapter.parse_trade({"E": 200, "t": 42, "T": 201, "p": "100.5", "q": "0.1", "m": 1})
    assert trade.event_time_ms == 200
    assert trade.trade_id == 42
    assert trade.trade_time_ms == 201
    assert trade.price == 100.5
    assert trade.qty == 0.1
    assert trade.is_buyer_maker == 1
    assert trade.side == "sell"


def test_kraken_adapter_parse_snapshot_and_update():
    from mm_recorder.exchanges.kraken import KrakenAdapter

    adapter = KrakenAdapter()
    payload = {
        "channel": "book",
        "type": "snapshot",
        "data": [
            {"symbol": "BTC/USD", "bids": [{"price": "100.0", "qty": "1.0"}], "asks": [{"price": "101.0", "qty": "2.0"}], "checksum": 5}
        ],
    }
    snapshots, diffs, trades = adapter.parse_ws_message(payload)
    assert len(snapshots) == 1
    assert snapshots[0].bids[0][0] == "100.0"

    payload = {
        "channel": "book",
        "type": "update",
        "data": [
            {"symbol": "BTC/USD", "bids": [{"price": "100.0", "qty": "1.0"}], "asks": [{"price": "101.0", "qty": "2.0"}], "checksum": 5}
        ],
    }
    snapshots, diffs, trades = adapter.parse_ws_message(payload)
    assert len(diffs) == 1
