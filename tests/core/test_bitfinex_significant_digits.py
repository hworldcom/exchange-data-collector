from __future__ import annotations

from mm_core.checksum.bitfinex import BitfinexBook


def test_bitfinex_significant_digits_rounding_large_price():
    book = BitfinexBook(depth=10, price_precision=5)
    book.load_snapshot(bids=[["67858.4", "1", "0.1"]], asks=[])
    bids, _ = book.top_n(1)
    assert bids[0][0] == 67858.0


def test_bitfinex_significant_digits_rounding_sub_1000():
    book = BitfinexBook(depth=10, price_precision=5)
    book.load_snapshot(bids=[["678.124", "1", "0.1"]], asks=[])
    bids, _ = book.top_n(1)
    assert bids[0][0] == 678.12
