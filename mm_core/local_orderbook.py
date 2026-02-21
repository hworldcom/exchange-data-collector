from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from itertools import islice
from typing import Iterable, List, Optional, Tuple

from sortedcontainers import SortedDict


_DEFAULT_TICK_SIZE = Decimal("0.01")


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _coerce_tick_size(value) -> Decimal:
    tick = _to_decimal(value)
    if tick <= 0:
        raise ValueError(f"tick_size must be positive (got {value!r})")
    return tick


def set_default_tick_size(tick_size) -> None:
    global _DEFAULT_TICK_SIZE
    _DEFAULT_TICK_SIZE = _coerce_tick_size(tick_size)


def get_default_tick_size() -> Decimal:
    return _DEFAULT_TICK_SIZE


@dataclass
class LocalOrderBook:
    """In-memory L2 book keyed by integer price ticks."""

    tick_size: Decimal | str | float | None = None
    bids: SortedDict = field(default_factory=SortedDict)
    asks: SortedDict = field(default_factory=SortedDict)
    last_update_id: Optional[int] = None

    def __post_init__(self) -> None:
        if self.tick_size is None:
            self.tick_size = get_default_tick_size()
        else:
            self.tick_size = _coerce_tick_size(self.tick_size)

    def _price_to_tick(self, price) -> int:
        p = _to_decimal(price)
        ticks = p / self.tick_size
        ticks_int = ticks.to_integral_value(rounding=ROUND_HALF_UP)
        if ticks != ticks_int:
            diff = abs(p - (ticks_int * self.tick_size))
            epsilon = self.tick_size * Decimal("0.000001")
            if diff > epsilon:
                raise ValueError(f"price {price!r} does not align to tick_size {self.tick_size}")
        return int(ticks_int)

    def _tick_to_price(self, tick: int) -> Decimal:
        return _to_decimal(tick) * self.tick_size

    def _apply_level(self, side: SortedDict, price, qty) -> None:
        tick = self._price_to_tick(price)
        qty_dec = _to_decimal(qty)
        if qty_dec == 0:
            side.pop(tick, None)
        else:
            side[tick] = float(qty_dec)

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        self.bids.clear()
        self.asks.clear()

        for price_snapshot, quantity_snapshot in bids:
            self._apply_level(self.bids, price_snapshot, quantity_snapshot)

        for price_snapshot, quantity_snapshot in asks:
            self._apply_level(self.asks, price_snapshot, quantity_snapshot)

        self.last_update_id = int(last_update_id)

    def replace_levels(
        self,
        bids: Iterable[Tuple[float | str | Decimal, float | str | Decimal]],
        asks: Iterable[Tuple[float | str | Decimal, float | str | Decimal]],
    ) -> None:
        self.bids.clear()
        self.asks.clear()
        for price, qty in bids:
            self._apply_level(self.bids, price, qty)
        for price, qty in asks:
            self._apply_level(self.asks, price, qty)

    def apply_diff(self, U: int, u: int, bids, asks) -> bool:
        """Apply a Binance diff-depth update.

        Returns:
          True  -> applied or safely ignored (stale)
          False -> sequence gap detected (book invalid; resync required)
        """
        if self.last_update_id is None:
            return False

        U = int(U)
        u = int(u)
        last = int(self.last_update_id)

        # stale event
        if u <= last:
            return True

        # gap
        if U > last + 1:
            return False

        # apply bids
        for price_snapshot, quantity_snapshot in bids:
            self._apply_level(self.bids, price_snapshot, quantity_snapshot)

        # apply asks
        for price_snapshot, quantity_snapshot in asks:
            self._apply_level(self.asks, price_snapshot, quantity_snapshot)

        self.last_update_id = u
        return True

    def iter_bids(self) -> Iterable[Tuple[float, float]]:
        for tick, qty in reversed(self.bids.items()):
            yield float(self._tick_to_price(tick)), qty

    def iter_asks(self) -> Iterable[Tuple[float, float]]:
        for tick, qty in self.asks.items():
            yield float(self._tick_to_price(tick)), qty

    def levels(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        return list(self.iter_bids()), list(self.iter_asks())

    def top_n(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        if n <= 0:
            return [], []
        bids_iter = ((float(self._tick_to_price(t)), q) for t, q in reversed(self.bids.items()))
        asks_iter = ((float(self._tick_to_price(t)), q) for t, q in self.asks.items())
        bids_sorted = list(islice(bids_iter, n))
        asks_sorted = list(islice(asks_iter, n))
        return bids_sorted, asks_sorted
