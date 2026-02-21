from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from .types import DepthDiff, Trade, Snapshot
from mm_core.sync_engine import OrderBookSyncEngine


class ExchangeAdapter(ABC):
    name: str
    sync_mode: str = "sequence"

    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def ws_url(self, symbol: str) -> str:
        raise NotImplementedError

    def subscribe_messages(self, symbol: str, depth: int) -> list:
        return []

    def normalize_depth(self, depth: int) -> int:
        return int(depth)

    def create_sync_engine(self, depth: int, **_kwargs):
        return OrderBookSyncEngine()

    @property
    def uses_custom_ws_messages(self) -> bool:
        return False

    def parse_ws_message(self, data: dict):
        raise NotImplementedError

    def parse_depth(self, data: dict) -> DepthDiff:
        raise NotImplementedError

    def parse_trade(self, data: dict) -> Trade:
        raise NotImplementedError

    # Snapshot fetching/writing is handled by recorder for now.
