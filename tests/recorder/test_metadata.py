from __future__ import annotations

import pytest

from mm_recorder.metadata import parse_bitfinex_assets


@pytest.mark.parametrize(
    ("raw_symbol", "symbol", "expected"),
    [
        ("BTC/USD", "tBTCUSD", ("BTC", "USD")),
        ("BTC-USD", "tBTCUSD", ("BTC", "USD")),
        ("BTC USD", "tBTCUSD", ("BTC", "USD")),
        (None, "tBTCUSD", ("BTC", "USD")),
        (None, "tDOGEUSD", ("DOGE", "USD")),
        (None, "tBTCUST", ("BTC", "UST")),
    ],
)
def test_parse_bitfinex_assets(raw_symbol, symbol, expected):
    assert parse_bitfinex_assets(symbol, raw_symbol=raw_symbol) == expected


def test_parse_bitfinex_assets_rejects_unknown_quote():
    with pytest.raises(RuntimeError, match="Could not infer Bitfinex base/quote assets"):
        parse_bitfinex_assets("tFOOBAR")
