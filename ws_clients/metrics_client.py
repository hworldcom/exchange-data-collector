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
    # SYMBOLS=BTCUSDC,ETHUSDC WINDOW=30d python ws_clients/metrics_client.py
    # METRICS_HOST=localhost METRICS_PORT=8766 EXCHANGE=binance INTERVAL=1m METRIC=correlation python ws_clients/metrics_client.py
    host = os.getenv("METRICS_HOST", "localhost")
    port = os.getenv("METRICS_PORT", "8766")
    exchange = os.getenv("EXCHANGE", "binance")
    symbols = _env("SYMBOLS", "BTCUSDC,ETHUSDC")
    interval = os.getenv("INTERVAL", "1m")
    window = os.getenv("WINDOW", "180d")
    metric = os.getenv("METRIC", "correlation")

    url = (
        f"ws://{host}:{port}/metrics?"
        f"exchange={exchange}&symbols={symbols}&interval={interval}&window={window}&metric={metric}"
    )
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
