from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Optional, Tuple


@dataclass(frozen=True)
class PageWindow:
    start_ms: int
    end_ms: int
    limit: int


def paginate_by_time(
    start_ms: int,
    end_ms: int,
    limit: int,
    step_ms: Optional[int] = None,
) -> Iterator[PageWindow]:
    """Yield time windows for REST pagination.

    If step_ms is None, caller is expected to use the exchange limit and
    advance using the last item timestamp.
    """
    if end_ms <= start_ms:
        return

    if step_ms is None:
        yield PageWindow(start_ms=start_ms, end_ms=end_ms, limit=limit)
        return

    cursor = start_ms
    while cursor < end_ms:
        next_end = min(cursor + step_ms, end_ms)
        yield PageWindow(start_ms=cursor, end_ms=next_end, limit=limit)
        cursor = next_end


def paginate_by_id(start_id: int, end_id: Optional[int], limit: int) -> Iterator[Tuple[int, int]]:
    """Yield (from_id, limit) pairs for ID-based pagination."""
    if end_id is not None and end_id < start_id:
        return

    current = start_id
    while True:
        yield current, limit
        current += limit
        if end_id is not None and current > end_id:
            break

