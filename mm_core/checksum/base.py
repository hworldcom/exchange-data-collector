from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class DepthDiff:
    event_time_ms: int
    U: int
    u: int
    bids: List[List[str]]
    asks: List[List[str]]
    checksum: Optional[int] = None
    raw: Optional[dict] = None


@dataclass(frozen=True)
class BookSnapshot:
    event_time_ms: int
    bids: List[List[str]]
    asks: List[List[str]]
    checksum: Optional[int] = None
    raw: Optional[dict] = None
