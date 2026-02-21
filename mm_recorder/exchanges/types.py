from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class DepthDiff:
    event_time_ms: int
    U: int
    u: int
    bids: List[List[str]] | List[List[float]] | Sequence
    asks: List[List[str]] | List[List[float]] | Sequence
    seq: Optional[int] = None
    prev_seq: Optional[int] = None
    checksum: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class Trade:
    event_time_ms: int
    trade_id: int
    trade_time_ms: int
    price: float
    qty: float
    is_buyer_maker: int
    side: Optional[str] = None
    ord_type: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class Snapshot:
    bids: List[List[str]]
    asks: List[List[str]]
    last_update_id: int
    raw: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class BookSnapshot:
    event_time_ms: int
    bids: List[List[str]]
    asks: List[List[str]]
    checksum: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None
