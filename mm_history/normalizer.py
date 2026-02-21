from __future__ import annotations

from typing import Any, Mapping

from mm_history.types import Candle, Trade


def normalize_candle(
    exchange: str,
    symbol: str,
    interval: str,
    ts_ms: int,
    open_: str,
    high: str,
    low: str,
    close: str,
    volume: str,
    raw: Mapping[str, Any] | None = None,
) -> Candle:
    return Candle(
        ts_ms=ts_ms,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        exchange=exchange,
        symbol=symbol,
        interval=interval,
        raw=raw,
    )


def normalize_trade(
    exchange: str,
    symbol: str,
    ts_ms: int,
    price: str,
    size: str,
    side: str | None,
    trade_id: str | None,
    raw: Mapping[str, Any] | None = None,
) -> Trade:
    return Trade(
        ts_ms=ts_ms,
        price=price,
        size=size,
        side=side,
        trade_id=trade_id,
        exchange=exchange,
        symbol=symbol,
        raw=raw,
    )

