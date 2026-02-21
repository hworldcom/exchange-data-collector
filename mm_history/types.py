from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class Candle:
    ts_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    exchange: str
    symbol: str
    interval: str
    raw: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class Trade:
    ts_ms: int
    price: str
    size: str
    side: Optional[str]
    trade_id: Optional[str]
    exchange: str
    symbol: str
    raw: Optional[Mapping[str, Any]] = None

