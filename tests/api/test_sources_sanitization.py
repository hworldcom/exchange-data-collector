from __future__ import annotations

import pytest

from mm_api.sources import resolve_latest_paths, sanitize_exchange, sanitize_symbol


def test_sanitize_exchange_rejects_traversal():
    with pytest.raises(ValueError):
        sanitize_exchange("../etc")
    with pytest.raises(ValueError):
        sanitize_exchange("binance/..")
    assert sanitize_exchange("binance") == "binance"


def test_sanitize_symbol_rejects_traversal():
    with pytest.raises(ValueError):
        sanitize_symbol("../etc")
    with pytest.raises(ValueError):
        sanitize_symbol("BTC/../../etc")
    assert sanitize_symbol("BTCUSDT") == "BTCUSDT"
    assert sanitize_symbol("BTC/USD") == "BTC/USD"


def test_resolve_latest_paths_rejects_invalid_params():
    with pytest.raises(ValueError):
        resolve_latest_paths("../etc", "BTCUSDT")
    with pytest.raises(ValueError):
        resolve_latest_paths("binance", "../etc")
