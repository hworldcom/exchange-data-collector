from __future__ import annotations

from typing import Any, Dict


def make_message(
    msg_type: str,
    exchange: str,
    symbol: str,
    ts_ms: int,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "type": msg_type,
        "exchange": exchange,
        "symbol": symbol,
        "ts_ms": ts_ms,
        "data": data,
    }

