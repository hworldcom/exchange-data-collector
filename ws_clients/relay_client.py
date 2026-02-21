from __future__ import annotations

import asyncio
import json
import os
from typing import Optional

import websockets


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing env var: {name}")
    return value


async def main() -> None:
    # Usage:
    # SYMBOL=BTCUSDC python ws_clients/relay_client.py
    # RELAY_HOST=localhost RELAY_PORT=8765 EXCHANGE=binance FROM=tail python ws_clients/relay_client.py
    host = os.getenv("RELAY_HOST", "localhost")
    port = os.getenv("RELAY_PORT", "8765")
    exchange = os.getenv("EXCHANGE", "binance")
    symbol = _env("SYMBOL", "BTCUSDC")
    from_mode = os.getenv("FROM", "tail")

    url = f"ws://{host}:{port}/ws?exchange={exchange}&symbol={symbol}&from={from_mode}"
    print(f"Connecting to {url}")
    async with websockets.connect(url) as ws:
        print("Connected.")
        async for message in ws:
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                print(message)
                continue
            print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
