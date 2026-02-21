from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Optional

from mm_history.types import Candle, Trade


class HistoricalClient(ABC):
    """Exchange-agnostic historical data interface (REST-based)."""

    name: str

    @abstractmethod
    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> Iterable[Candle]:
        """Return candles in ascending time order."""

    @abstractmethod
    def fetch_trades(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> Iterable[Trade]:
        """Return trades in ascending time order."""

    def supports_interval(self, interval: str) -> bool:
        """Optional validation hook for exchange-specific intervals."""
        return True

    def normalize_symbol(self, symbol: str) -> str:
        """Optional hook to convert user symbols into exchange format."""
        return symbol

    def max_candle_limit(self) -> Optional[int]:
        """Return max candles per request if the exchange enforces it."""
        return None

    def max_trade_limit(self) -> Optional[int]:
        """Return max trades per request if the exchange enforces it."""
        return None

