from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List

from .base import ExchangeAdapter
from .types import BookSnapshot, DepthDiff, Trade
from mm_core.checksum.bitfinex import BitfinexSyncEngine


def _to_ms(ts: Any) -> int:
    if ts is None:
        return 0
    try:
        val = float(ts)
    except Exception:
        return 0
    # Bitfinex v2 generally uses ms; fall back to seconds if small.
    return int(val if val > 1e12 else val * 1000)


class BitfinexAdapter(ExchangeAdapter):
    name = "bitfinex"
    sync_mode = "checksum"

    def __init__(self) -> None:
        self.book_chan_id: int | None = None
        self.trades_chan_id: int | None = None

    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.replace("/", "").replace("-", "").replace(":", "").strip().upper()
        if not s:
            return s
        # Bitfinex pairs use leading "t" (lowercase)
        if s.startswith("T"):
            s = s[1:]
        return f"t{s}"

    def normalize_depth(self, depth: int) -> int:
        # Bitfinex checksum uses top 25 bids/asks.
        return 25

    def ws_url(self, symbol: str) -> str:
        return "wss://api.bitfinex.com/ws/2"

    def subscribe_messages(self, symbol: str, depth: int) -> list:
        pair = self.normalize_symbol(symbol)
        return [
            {"event": "conf", "flags": 131072},
            {"event": "subscribe", "channel": "book", "pair": pair, "prec": "P0", "freq": "F0", "len": 25},
            {"event": "subscribe", "channel": "trades", "pair": pair},
        ]

    @property
    def uses_custom_ws_messages(self) -> bool:
        return True

    def create_sync_engine(self, depth: int, price_precision: int | None = None, **_kwargs):
        return BitfinexSyncEngine(depth=25, price_precision=price_precision)

    def parse_ws_message(self, data: dict | list):
        snapshots: List[BookSnapshot] = []
        diffs: List[DepthDiff] = []
        trades: List[Trade] = []

        if isinstance(data, dict):
            if data.get("event") == "subscribed":
                chan_id = data.get("chanId")
                channel = data.get("channel")
                if channel == "book":
                    self.book_chan_id = int(chan_id)
                elif channel == "trades":
                    self.trades_chan_id = int(chan_id)
            return snapshots, diffs, trades

        if not isinstance(data, list) or not data:
            return snapshots, diffs, trades

        chan_id = data[0]
        if chan_id == self.book_chan_id:
            if len(data) >= 2 and data[1] == "hb":
                return snapshots, diffs, trades
            if len(data) >= 2 and data[1] == "cs":
                diffs.append(
                    DepthDiff(
                        event_time_ms=0,
                        U=0,
                        u=0,
                        bids=[],
                        asks=[],
                        checksum=int(data[2]),
                        raw={"type": "checksum", "checksum": int(data[2])},
                    )
                )
                return snapshots, diffs, trades
            # Snapshot: [chanId, [ [price, count, amount], ... ]]
            if len(data) == 2 and isinstance(data[1], list) and data[1] and isinstance(data[1][0], list):
                bids: List[List[str]] = []
                asks: List[List[str]] = []
                for price, count, amount in data[1]:
                    amount_val = Decimal(str(amount))
                    price_str = str(price)
                    amount_str = str(amount)
                    count_str = str(count)
                    if amount_str.startswith("-"):
                        amount_str = amount_str[1:]
                    row = [price_str, count_str, amount_str]
                    if amount_val > 0:
                        bids.append(row)
                    else:
                        asks.append(row)
                snapshots.append(
                    BookSnapshot(
                        event_time_ms=0,
                        bids=bids,
                        asks=asks,
                        checksum=None,
                        raw={"type": "snapshot"},
                    )
                )
                return snapshots, diffs, trades

            # Update: [chanId, [price, count, amount]] or [chanId, price, count, amount]
            if len(data) == 2 and isinstance(data[1], list) and data[1] and not isinstance(data[1][0], list):
                price, count, amount_raw = data[1][0], int(data[1][1]), data[1][2]
            elif len(data) >= 4:
                price, count, amount_raw = data[1], int(data[2]), data[3]
            else:
                price = None
                count = 0
                amount_raw = None

            if price is not None:
                amount = Decimal(str(amount_raw))
                price_str = str(price)
                amount_str = str(amount_raw)
                count_str = str(count)
                if count == 0:
                    if amount < 0:
                        asks = [[price_str, count_str, "0"]]
                        bids = []
                    else:
                        bids = [[price_str, count_str, "0"]]
                        asks = []
                elif amount > 0:
                    if amount_str.startswith("-"):
                        amount_str = amount_str[1:]
                    bids = [[price_str, count_str, amount_str]]
                    asks = []
                else:
                    if amount_str.startswith("-"):
                        amount_str = amount_str[1:]
                    asks = [[price_str, count_str, amount_str]]
                    bids = []
                diffs.append(
                    DepthDiff(
                        event_time_ms=0,
                        U=0,
                        u=0,
                        bids=bids,
                        asks=asks,
                        checksum=None,
                        raw={"type": "update", "price": str(price), "count": count, "amount": str(amount)},
                    )
                )
                return snapshots, diffs, trades

        if chan_id == self.trades_chan_id:
            if len(data) < 2:
                return snapshots, diffs, trades
            if data[1] == "hb":
                return snapshots, diffs, trades
            # Snapshot: [chanId, [ [trade_id, mts, amount, price], ... ]]
            if len(data) == 2 and isinstance(data[1], list):
                for entry in data[1]:
                    if not isinstance(entry, list):
                        continue
                    if len(entry) == 4:
                        trade_id, ts, amount, price = entry
                    elif len(entry) >= 5:
                        trade_id, ts, amount, price = entry[0:4]
                    else:
                        continue
                    amount_val = float(amount)
                    trade_time_ms = _to_ms(ts)
                    trades.append(
                        Trade(
                            event_time_ms=trade_time_ms,
                            trade_id=int(trade_id),
                            trade_time_ms=trade_time_ms,
                            price=float(price),
                            qty=abs(amount_val),
                            is_buyer_maker=0 if amount_val > 0 else 1,
                            side="buy" if amount_val > 0 else "sell",
                            raw={"type": "snapshot", "entry": entry},
                        )
                    )
                return snapshots, diffs, trades

            # Updates: [chanId, 'te'|'tu', trade_id, mts, amount, price]
            if len(data) >= 3 and data[1] in ("te", "tu"):
                if data[1] != "tu":
                    return snapshots, diffs, trades
                if len(data) < 6:
                    return snapshots, diffs, trades
                trade_id = data[2]
                ts = data[3]
                amount = data[4]
                price = data[5]
                amount_val = float(amount)
                trade_time_ms = _to_ms(ts)
                trades.append(
                    Trade(
                        event_time_ms=trade_time_ms,
                        trade_id=int(trade_id) if trade_id is not None else 0,
                        trade_time_ms=trade_time_ms,
                        price=float(price),
                        qty=abs(amount_val),
                        is_buyer_maker=0 if amount_val > 0 else 1,
                        side="buy" if amount_val > 0 else "sell",
                        raw={"type": data[1], "entry": data},
                    )
                )
                return snapshots, diffs, trades

        return snapshots, diffs, trades

    def parse_depth(self, data: dict) -> DepthDiff:
        raise RuntimeError("Bitfinex uses custom WS parsing; parse_depth is unused.")

    def parse_trade(self, data: dict) -> Trade:
        raise RuntimeError("Bitfinex uses custom WS parsing; parse_trade is unused.")
