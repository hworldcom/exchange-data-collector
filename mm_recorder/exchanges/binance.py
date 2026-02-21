from __future__ import annotations

from typing import Optional

from .base import ExchangeAdapter
from .types import DepthDiff, Trade


class BinanceAdapter(ExchangeAdapter):
    name = "binance"

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.upper().strip()

    def ws_url(self, symbol: str) -> str:
        sym = self.normalize_symbol(symbol).lower()
        return f"wss://stream.binance.com:9443/stream?streams={sym}@depth@100ms/{sym}@trade"

    def parse_depth(self, data: dict) -> DepthDiff:
        return DepthDiff(
            event_time_ms=int(data.get("E", 0)),
            U=int(data.get("U", 0)),
            u=int(data.get("u", 0)),
            bids=data.get("b", []),
            asks=data.get("a", []),
            raw=data,
        )

    def parse_trade(self, data: dict) -> Trade:
        is_buyer_maker = int(data.get("m", 0))
        side = "sell" if is_buyer_maker == 1 else "buy"
        return Trade(
            event_time_ms=int(data.get("E", 0)),
            trade_id=int(data.get("t", 0)),
            trade_time_ms=int(data.get("T", 0)),
            price=float(data.get("p", 0)),
            qty=float(data.get("q", 0)),
            is_buyer_maker=is_buyer_maker,
            side=side,
            raw=data,
        )
