from __future__ import annotations

from typing import Any, Dict, List, Tuple
from datetime import datetime
from decimal import Decimal

from .base import ExchangeAdapter
from .types import DepthDiff, Trade, BookSnapshot
from mm_core.checksum.kraken import KrakenSyncEngine


def _as_level_list(levels: List[Dict[str, Any]] | List[List[str]]) -> List[List[str]]:
    out: List[List[str]] = []
    for lv in levels or []:
        if isinstance(lv, dict):
            price = str(lv.get("price"))
            qty = str(lv.get("qty"))
        else:
            if len(lv) < 2:
                continue
            price = str(lv[0])
            qty = str(lv[1])
        out.append([price, qty])
    return out


def _parse_event_ms(ts: Any) -> int:
    if ts is None:
        return 0
    if isinstance(ts, (int, float, Decimal)):
        return int(float(ts) * 1000)
    if isinstance(ts, str):
        try:
            return int(float(ts) * 1000)
        except ValueError:
            try:
                return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                return 0
    return 0


class KrakenAdapter(ExchangeAdapter):
    name = "kraken"
    sync_mode = "checksum"
    _allowed_depths = (10, 25, 100, 500, 1000)

    def _select_depth(self, depth: int) -> int:
        if depth in self._allowed_depths:
            return depth
        for candidate in self._allowed_depths:
            if depth <= candidate:
                return candidate
        return self._allowed_depths[-1]

    def normalize_depth(self, depth: int) -> int:
        return self._select_depth(int(depth))

    def create_sync_engine(self, depth: int, **_kwargs):
        return KrakenSyncEngine(depth)

    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.strip().upper()
        if "/" in s:
            return s
        if "-" in s:
            base, quote = s.split("-", 1)
            return f"{base}/{quote}"
        if len(s) >= 6:
            # Best-effort split for common pairs like BTCUSD -> BTC/USD
            return f"{s[:-3]}/{s[-3:]}"
        return s

    def ws_url(self, symbol: str) -> str:
        return "wss://ws.kraken.com/v2"

    def subscribe_messages(self, symbol: str, depth: int) -> list:
        depth = self.normalize_depth(depth)
        return [
            {
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": [self.normalize_symbol(symbol)],
                    "depth": int(depth),
                    "snapshot": True,
                },
            },
            {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": [self.normalize_symbol(symbol)],
                    "snapshot": True,
                },
            }
        ]

    @property
    def uses_custom_ws_messages(self) -> bool:
        return True

    def parse_ws_message(self, data: dict):
        # Kraken v2: {"channel":"book","type":"snapshot|update", "data":[{...}]}
        snapshots: List[BookSnapshot] = []
        diffs: List[DepthDiff] = []
        trades: List[Trade] = []

        if not isinstance(data, dict):
            return snapshots, diffs, trades

        channel = data.get("channel")
        msg_type = data.get("type")
        if channel == "book":
            for entry in data.get("data", []) or []:
                bids = _as_level_list(entry.get("bids", []))
                asks = _as_level_list(entry.get("asks", []))
                checksum = entry.get("checksum")
                event_ms = _parse_event_ms(entry.get("timestamp"))

                raw_payload = {"channel": channel, "type": msg_type, "data": entry}
                if msg_type == "snapshot":
                    snapshots.append(
                        BookSnapshot(
                            event_time_ms=event_ms,
                            bids=bids,
                            asks=asks,
                            checksum=int(checksum) if checksum is not None else None,
                            raw=raw_payload,
                        )
                    )
                elif msg_type == "update":
                    diffs.append(
                        DepthDiff(
                            event_time_ms=event_ms,
                            U=0,
                            u=0,
                            bids=bids,
                            asks=asks,
                            checksum=int(checksum) if checksum is not None else None,
                            raw=raw_payload,
                        )
                    )
        elif channel == "trade":
            for idx, entry in enumerate(data.get("data", []) or []):
                event_ms = _parse_event_ms(entry.get("timestamp"))
                trade_id = entry.get("trade_id")
                side = str(entry.get("side") or "").lower()
                is_buyer_maker = 0 if side == "buy" else 1
                if trade_id is None:
                    trade_id = int(event_ms * 1000 + idx)
                raw_payload = {"channel": channel, "type": msg_type, "data": entry}
                trades.append(
                    Trade(
                        event_time_ms=event_ms,
                        trade_id=int(trade_id) if trade_id is not None else 0,
                        trade_time_ms=event_ms,
                        price=float(entry.get("price") or 0.0),
                        qty=float(entry.get("qty") or 0.0),
                        is_buyer_maker=is_buyer_maker,
                        side=side or None,
                        ord_type=str(entry.get("ord_type")) if entry.get("ord_type") is not None else None,
                        raw=raw_payload,
                    )
                )

        return snapshots, diffs, trades

    def parse_depth(self, data: dict) -> DepthDiff:
        raise RuntimeError("Kraken uses custom WS parsing; parse_depth is unused.")

    def parse_trade(self, data: dict) -> Trade:
        raise RuntimeError("Kraken trades not implemented yet.")
