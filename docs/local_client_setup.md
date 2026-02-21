# Local Client Setup (Relay + Metrics)

This guide shows how to run the recorder and connect local clients to the relay
and metrics WebSocket servers.

## 1) Run the recorder

```bash
EXCHANGE=binance SYMBOL=BTCUSDC python -m mm_recorder.recorder
```

Optional: disable gzip events/tail and use only live files:
```bash
WS_RELAY_LIVE_ONLY=1
```

## 2) Run the WebSocket relay

```bash
python -m mm_api.relay
```

Or with Docker Compose:
```bash
docker compose up --build -d
```

Connect a local relay client:
```bash
SYMBOL=BTCUSDC python ws_clients/relay_client.py
```

## 3) Run the metrics server

```bash
python -m mm_api.metrics
```

Connect a local metrics client:
```bash
SYMBOLS=BTCUSDC,ETHUSDC WINDOW=30d python ws_clients/metrics_client.py
```

## 4) Run the REST snapshot endpoint (optional)

```bash
python -m mm_api.rest
```

Example request:
```
http://localhost:8080/snapshot?exchange=binance&symbol=BTCUSDC
```

## Notes
- Metrics use **1m candles** and update **every 1 second** with partial candles from live trades.
- If the metrics server is slow on first run, it is likely fetching a large history window.
  Reduce the window with `WINDOW=7d` or prefetch candles via `mm_history.cli`.
- If live updates stop, ensure the recorder is still running and that the **latest day**
  folder is being tailed (e.g., `data/binance/BTCUSDC/<YYYYMMDD>/live/`).

## Default streamer settings

### Relay
- `WS_RELAY_POLL_INTERVAL_S=1.0` — relay poll loop cadence.
- `WS_RELAY_LEVELS=20` — number of book levels for `type="levels"`.
- `WS_RELAY_LEVELS_INTERVAL_S=1.0` — emit `levels` every N seconds.
- `WS_RELAY_LIVE_ONLY=0` — when set to `1`, skip gzip fallbacks and tail only `live/` files.
- `WS_RELAY_VOLUME_WINDOW_S=86400` — rolling window for `volume_24h` (seconds).
- `WS_RELAY_VOLUME_INTERVAL_S=1.0` — emit `volume_24h` every N seconds.

### Metrics
- `METRICS_POLL_INTERVAL_S=1.0` — metrics update cadence.
- `METRICS_CACHE_HISTORY=1` — cache fetched history to `data/.../history/`.

## What the client receives

### Relay (`/ws`)
The relay streams JSON messages with:
- `type="snapshot"` — initial snapshot payload (if available)
- `type="diff"` — order book diffs
- `type="trade"` — raw trade payloads
- `type="event"` — recorder events (unless `WS_RELAY_LIVE_ONLY=1`)
- `type="spread"` — derived top-of-book spread:
  - `bid`, `ask`, `mid`, `spread_abs`, `spread_bps`
- `type="levels"` — top-N resting volume per level:
  - `bids`, `asks`, `sum_bid_qty`, `sum_ask_qty`
- `type="volume_24h"` — rolling traded volume:
  - `buy_volume`, `sell_volume`, `total_volume`

Message schema reference: `docs/ws_relay.md`.

### Metrics (`/metrics`)
The metrics server streams:
- `type="metric"` with:
  - `metric="correlation"` (two symbols) or `metric="volatility"` (one symbol)
  - `value` (float)
  - `interval`, `window_ms`

It also sends an initial status message:
- `metric="status"`, `value=1.0`
