from __future__ import annotations

import binascii
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import SyncResult
from .base import DepthDiff, BookSnapshot


def _signed_crc(val: int) -> int:
    return val - 2**32 if val > 2**31 - 1 else val


class BitfinexBook:
    def __init__(self, depth: int, price_precision: int | None = None) -> None:
        # price -> (price_str, count_str, amount_str)
        self.bids: Dict[Decimal, tuple[str, str, str]] = {}
        self.asks: Dict[Decimal, tuple[str, str, str]] = {}
        self.depth = int(depth)
        self.price_precision = int(price_precision) if price_precision is not None else None

    def _normalize_price(self, price: str | Decimal) -> Decimal:
        px = Decimal(str(price))
        if self.price_precision is None:
            return px
        if px == 0:
            return px
        adj = px.copy_abs().adjusted()
        step = Decimal(1).scaleb(adj - (self.price_precision - 1))
        return (px / step).to_integral_value(rounding=ROUND_HALF_UP) * step

    def _trim(self) -> None:
        if self.depth <= 0:
            return
        if len(self.bids) > self.depth:
            bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[: self.depth]
            self.bids = {p: q for p, q in bids_sorted}
        if len(self.asks) > self.depth:
            asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[: self.depth]
            self.asks = {p: q for p, q in asks_sorted}

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]]) -> None:
        self.bids.clear()
        self.asks.clear()
        for row in bids:
            if not row:
                continue
            if len(row) >= 3:
                price_str, count_str, amount_str = str(row[0]), str(row[1]), str(row[2])
            else:
                price_str, amount_str = str(row[0]), str(row[1])
                count_str = "1"
            if amount_str.startswith("-"):
                amount_str = amount_str[1:]
            price_norm = self._normalize_price(price_str)
            self.bids[price_norm] = (str(price_norm), count_str, amount_str)
        for row in asks:
            if not row:
                continue
            if len(row) >= 3:
                price_str, count_str, amount_str = str(row[0]), str(row[1]), str(row[2])
            else:
                price_str, amount_str = str(row[0]), str(row[1])
                count_str = "1"
            if amount_str.startswith("-"):
                amount_str = amount_str[1:]
            price_norm = self._normalize_price(price_str)
            self.asks[price_norm] = (str(price_norm), count_str, amount_str)
        self._trim()

    def apply_update(self, price: str, count: int, amount: str) -> None:
        px = self._normalize_price(price)
        amt = Decimal(amount)
        if count == 0:
            if amt < 0:
                self.asks.pop(px, None)
            else:
                self.bids.pop(px, None)
            return
        if amt < 0:
            amt_str = str(amount)
            if amt_str.startswith("-"):
                amt_str = amt_str[1:]
            self.asks[px] = (str(px), str(count), amt_str)
        else:
            amt_str = str(amount)
            if amt_str.startswith("-"):
                amt_str = amt_str[1:]
            self.bids[px] = (str(px), str(count), amt_str)
        self._trim()

    def top_n(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return (
            [(float(p), abs(float(Decimal(q[2])))) for p, q in bids_sorted],
            [(float(p), abs(float(Decimal(q[2])))) for p, q in asks_sorted],
        )

    def checksum(self, n: int = 25, abs_asks: bool = False, abs_all: bool = False, interleave: bool = True) -> int:
        payload = self.checksum_payload(n, abs_asks=abs_asks, abs_all=abs_all, interleave=interleave).encode()
        return _signed_crc(binascii.crc32(payload) & 0xFFFFFFFF)

    def checksum_payload(
        self, n: int = 25, abs_asks: bool = False, abs_all: bool = False, interleave: bool = True
    ) -> str:
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        parts: List[str] = []
        if interleave:
            for i in range(n):
                if i < len(bids_sorted):
                    _, (p_str, _c_str, a_str) = bids_sorted[i]
                    if abs_all and a_str.startswith("-"):
                        a_str = a_str[1:]
                    parts.append(p_str)
                    parts.append(a_str)
                if i < len(asks_sorted):
                    _, (p_str, _c_str, a_str) = asks_sorted[i]
                    if (abs_all or abs_asks) and a_str.startswith("-"):
                        a_str = a_str[1:]
                    if not a_str.startswith("-"):
                        a_str = f"-{a_str}"
                    parts.append(p_str)
                    parts.append(a_str)
        else:
            for _, (p_str, _c_str, a_str) in bids_sorted:
                if abs_all and a_str.startswith("-"):
                    a_str = a_str[1:]
                parts.append(p_str)
                parts.append(a_str)
            for _, (p_str, _c_str, a_str) in asks_sorted:
                if (abs_all or abs_asks) and a_str.startswith("-"):
                    a_str = a_str[1:]
                if not a_str.startswith("-"):
                    a_str = f"-{a_str}"
                parts.append(p_str)
                parts.append(a_str)
        return ":".join(parts)


@dataclass
class BitfinexSyncEngine:
    lob: LocalOrderBook
    depth_synced: bool = False
    snapshot_loaded: bool = False
    buffer: List[DepthDiff] = None
    last_checksum_payload: Optional[str] = None

    def __init__(self, depth: int, max_buffer_size: int = 200_000, price_precision: int | None = None) -> None:
        self.depth = int(depth)
        self.price_precision = int(price_precision) if price_precision is not None else None
        self.book = BitfinexBook(self.depth, price_precision=self.price_precision)
        self.lob = LocalOrderBook()
        self.tick_size = self.lob.tick_size
        self.depth_synced = False
        self.snapshot_loaded = False
        self.buffer = []
        self.last_recv_seq: Optional[int] = None
        self.last_checksum_payload = None
        self.max_buffer_size = int(max_buffer_size) if max_buffer_size is not None else None

    def adopt_snapshot(self, snapshot: BookSnapshot) -> None:
        self.book.load_snapshot(snapshot.bids, snapshot.asks)
        bids, asks = self.book.top_n(self.depth)
        self.lob.replace_levels(bids, asks)
        self.snapshot_loaded = True
        self.depth_synced = True
        if self.buffer:
            for ev in list(self.buffer):
                self.feed_depth_event(ev)
            self.buffer.clear()

    def reset_for_resync(self) -> None:
        self.book = BitfinexBook(self.depth, price_precision=self.price_precision)
        self.lob = LocalOrderBook(tick_size=self.tick_size)
        self.snapshot_loaded = False
        self.depth_synced = False
        self.buffer.clear()

    def feed_depth_event(self, ev: DepthDiff) -> SyncResult:
        if not self.snapshot_loaded:
            self.buffer.append(ev)
            if self.max_buffer_size and len(self.buffer) > self.max_buffer_size:
                self.buffer.clear()
                return SyncResult("gap", "buffer_overflow")
            return SyncResult("buffered", "no_snapshot")

        raw = ev.raw or {}
        if raw.get("type") == "update":
            self.book.apply_update(raw.get("price", "0"), int(raw.get("count", 0)), raw.get("amount", "0"))
            bids, asks = self.book.top_n(self.depth)
            self.lob.replace_levels(bids, asks)

        if ev.checksum is not None or raw.get("type") == "checksum":
            calc = self.book.checksum(25)
            expected = int(ev.checksum) if ev.checksum is not None else int(raw.get("checksum", 0))
            if int(calc) != int(expected):
                alt_non = self.book.checksum(25, interleave=False)
                if int(alt_non) == int(expected):
                    return SyncResult("applied", "checksum_ok_non_interleaved")
                payload = self.book.checksum_payload(25)
                self.last_checksum_payload = payload
                preview = payload[:200]
                return SyncResult(
                    "gap",
                    f"checksum_mismatch expected={expected} got={calc} payload={preview}",
                )

        return SyncResult("applied", "checksum_ok")
