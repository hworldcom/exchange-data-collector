from __future__ import annotations

from mm_api.relay import _RollingVolume, _TopOfBook


def test_top_of_book_updates() -> None:
    book = _TopOfBook()
    book.apply_updates([("100", "1.0")], [("101", "1.0")])
    assert book.best_bid == 100.0
    assert book.best_ask == 101.0

    book.apply_updates([("102", "2.0")], [])
    assert book.best_bid == 102.0

    book.apply_updates([], [("100.5", "1.0")])
    assert book.best_ask == 100.5

    book.apply_updates([("102", "0")], [])
    assert book.best_bid == 100.0


def test_top_levels() -> None:
    book = _TopOfBook()
    book.apply_updates([("100", "1.0"), ("99", "2.0")], [("101", "1.0"), ("102", "3.0")])
    bids, asks = book.top_levels(1)
    assert bids[0][0] == 100.0
    assert asks[0][0] == 101.0


def test_rolling_volume() -> None:
    vol = _RollingVolume(window_s=3)
    vol.add(1000, 1.0, "buy")
    vol.add(2000, 2.0, "sell")
    vol.add(3000, 3.0, "buy")
    buy, sell = vol.totals()
    assert buy == 4.0
    assert sell == 2.0
    vol.add(5000, 1.0, "sell")
    buy, sell = vol.totals()
    assert buy == 3.0
    assert sell == 1.0
