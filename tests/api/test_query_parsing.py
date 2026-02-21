from __future__ import annotations

from mm_api import relay as relay_mod
from mm_api import metrics as metrics_mod


def test_parse_query_handles_bare_flags_relay():
    params = relay_mod._parse_query("/ws?symbol=BTC&verbose")
    assert params["symbol"] == "BTC"
    assert "verbose" in params
    assert params["verbose"] == ""


def test_parse_query_handles_bare_flags_metrics():
    params = metrics_mod._parse_query("/metrics?symbol=BTC&verbose")
    assert params["symbol"] == "BTC"
    assert "verbose" in params
    assert params["verbose"] == ""
