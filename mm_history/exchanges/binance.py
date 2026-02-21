from __future__ import annotations

from typing import Iterable, List, Optional

import logging
import requests

from mm_history.exchanges.base import HistoricalClient
from mm_history.normalizer import normalize_candle, normalize_trade
from mm_history.types import Candle, Trade


_BINANCE_REST = "https://api.binance.com"
_KLINES_PATH = "/api/v3/klines"
_MAX_LIMIT = 1000
_AGGTRADES_PATH = "/api/v3/aggTrades"


class BinanceHistoricalClient(HistoricalClient):
    name = "binance"
    _log = logging.getLogger("mm_history.binance")

    def supports_interval(self, interval: str) -> bool:
        return interval in {
            "1s",
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        }

    def max_candle_limit(self) -> int:
        return _MAX_LIMIT

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").replace("-", "").replace(":", "").replace(" ", "").upper()

    def fetch_candles(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> Iterable[Candle]:
        if not self.supports_interval(interval):
            raise ValueError(f"Unsupported Binance interval: {interval}")

        symbol = self.normalize_symbol(symbol)
        limit = min(limit, _MAX_LIMIT)
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        self._log.info("Fetch candles %s %s start=%s end=%s limit=%s", symbol, interval, start_ms, end_ms, limit)
        resp = requests.get(f"{_BINANCE_REST}{_KLINES_PATH}", params=params, timeout=30)
        resp.raise_for_status()
        payload: List[list] = resp.json()
        candles: List[Candle] = []
        for row in payload:
            open_time = int(row[0])
            candles.append(
                normalize_candle(
                    exchange=self.name,
                    symbol=symbol,
                    interval=interval,
                    ts_ms=open_time,
                    open_=str(row[1]),
                    high=str(row[2]),
                    low=str(row[3]),
                    close=str(row[4]),
                    volume=str(row[5]),
                    raw={
                        "open_time": row[0],
                        "close_time": row[6],
                        "quote_volume": row[7],
                        "trade_count": row[8],
                        "taker_buy_base_volume": row[9],
                        "taker_buy_quote_volume": row[10],
                    },
                )
            )
        return candles

    def fetch_trades(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int,
    ) -> Iterable[Trade]:
        symbol = self.normalize_symbol(symbol)
        limit = min(limit, _MAX_LIMIT)
        params = {
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        self._log.info("Fetch aggTrades %s start=%s end=%s limit=%s", symbol, start_ms, end_ms, limit)
        resp = requests.get(f"{_BINANCE_REST}{_AGGTRADES_PATH}", params=params, timeout=30)
        resp.raise_for_status()
        payload: List[dict] = resp.json()
        trades: List[Trade] = []
        for row in payload:
            trades.append(
                normalize_trade(
                    exchange=self.name,
                    symbol=symbol,
                    ts_ms=int(row["T"]),
                    price=str(row["p"]),
                    size=str(row["q"]),
                    side="sell" if row.get("m") else "buy",
                    trade_id=str(row.get("a")) if row.get("a") is not None else None,
                    raw=row,
                )
            )
        return trades
