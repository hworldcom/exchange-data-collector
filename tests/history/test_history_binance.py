from __future__ import annotations

from typing import Any, Dict, List

import pytest

from mm_history.exchanges.binance import BinanceHistoricalClient


class _FakeResponse:
    def __init__(self, payload: Any, status: int = 200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._payload


def _mock_get(expected_url: str, payload: Any, params_out: Dict[str, Any]):
    def _get(url: str, params: Dict[str, Any], timeout: int):
        assert url == expected_url
        params_out.update(params)
        assert timeout == 30
        return _FakeResponse(payload)

    return _get


def test_binance_fetch_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BinanceHistoricalClient()
    payload: List[list] = [
        [
            1700000000000,
            "10.0",
            "11.0",
            "9.0",
            "10.5",
            "123.45",
            1700000059999,
            "1000.0",
            42,
            "500.0",
            "600.0",
            "0",
        ]
    ]
    params_out: Dict[str, Any] = {}
    monkeypatch.setattr(
        "mm_history.exchanges.binance.requests.get",
        _mock_get("https://api.binance.com/api/v3/klines", payload, params_out),
    )
    candles = list(
        client.fetch_candles(
            symbol="BTCUSDT",
            interval="1m",
            start_ms=1700000000000,
            end_ms=1700000060000,
            limit=1000,
        )
    )
    assert len(candles) == 1
    candle = candles[0]
    assert candle.ts_ms == 1700000000000
    assert candle.open == "10.0"
    assert candle.high == "11.0"
    assert candle.low == "9.0"
    assert candle.close == "10.5"
    assert candle.volume == "123.45"
    assert candle.exchange == "binance"
    assert candle.symbol == "BTCUSDT"
    assert candle.interval == "1m"
    assert params_out["symbol"] == "BTCUSDT"
    assert params_out["interval"] == "1m"
    assert params_out["startTime"] == 1700000000000
    assert params_out["endTime"] == 1700000060000
    assert params_out["limit"] == 1000


def test_binance_fetch_trades(monkeypatch: pytest.MonkeyPatch) -> None:
    client = BinanceHistoricalClient()
    payload = [
        {
            "a": 100,
            "p": "100.0",
            "q": "0.5",
            "f": 1,
            "l": 1,
            "T": 1700000000123,
            "m": True,
            "M": True,
        },
        {
            "a": 101,
            "p": "101.0",
            "q": "0.25",
            "f": 2,
            "l": 2,
            "T": 1700000001123,
            "m": False,
            "M": True,
        },
    ]
    params_out: Dict[str, Any] = {}
    monkeypatch.setattr(
        "mm_history.exchanges.binance.requests.get",
        _mock_get("https://api.binance.com/api/v3/aggTrades", payload, params_out),
    )
    trades = list(
        client.fetch_trades(
            symbol="BTCUSDT",
            start_ms=1700000000000,
            end_ms=1700000002000,
            limit=500,
        )
    )
    assert len(trades) == 2
    assert trades[0].trade_id == "100"
    assert trades[0].price == "100.0"
    assert trades[0].size == "0.5"
    assert trades[0].side == "sell"
    assert trades[1].trade_id == "101"
    assert trades[1].side == "buy"
    assert params_out["symbol"] == "BTCUSDT"
    assert params_out["startTime"] == 1700000000000
    assert params_out["endTime"] == 1700000002000
    assert params_out["limit"] == 500

