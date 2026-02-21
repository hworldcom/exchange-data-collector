# Client Connection Guide (Local)

This guide explains how to connect client applications to the local relay and metrics servers.

## Endpoints

### Relay (live market data)
```
ws://localhost:8765/ws?exchange=binance&symbol=BTCUSDC
```

Query params:
- `exchange`: `binance`, `kraken`, `bitfinex`
- `symbol`: trading pair (e.g., `BTCUSDC`, `BTCUSDT`, `BTC/USD`)
- `from`: `tail` (default) or `start`

### Metrics (server-computed)
```
ws://localhost:8766/metrics?exchange=binance&symbols=BTCUSDC,ETHUSDC&interval=1m&window=30d&metric=correlation
```

Query params:
- `exchange`: currently `binance`
- `symbols`: comma-separated list
- `interval`: `1m` (current default)
- `window`: e.g. `7d`, `30d`, `180d`
- `metric`: `correlation` or `volatility`

## Message Types

Relay stream (`/ws`) emits:
- `snapshot` — latest snapshot (if available)
- `diff` — order book diffs
- `trade` — trade prints
- `event` — recorder events (unless `WS_RELAY_LIVE_ONLY=1`)
- `spread` — top-of-book spread
- `levels` — top-N levels with resting volume
- `volume_24h` — rolling traded volume (buy/sell/total)

Metrics stream (`/metrics`) emits:
- `metric` — correlation/volatility updates
- `metric=status` — handshake confirmation

## Example payloads

Relay `spread`:
```
{
  "type": "spread",
  "exchange": "binance",
  "symbol": "BTCUSDC",
  "ts_ms": 1700000000123,
  "data": {
    "bid": 50000.0,
    "ask": 50001.0,
    "mid": 50000.5,
    "spread_abs": 1.0,
    "spread_bps": 0.2
  }
}
```

Relay `levels`:
```
{
  "type": "levels",
  "exchange": "binance",
  "symbol": "BTCUSDC",
  "ts_ms": 1700000000123,
  "data": {
    "levels": 20,
    "bids": [[50000.0, 1.2], [49999.5, 0.8]],
    "asks": [[50001.0, 0.9], [50001.5, 1.1]],
    "sum_bid_qty": 2.0,
    "sum_ask_qty": 2.0
  }
}
```

Relay `volume_24h`:
```
{
  "type": "volume_24h",
  "exchange": "binance",
  "symbol": "BTCUSDC",
  "ts_ms": 1700000000123,
  "data": {
    "window_s": 86400,
    "buy_volume": 123.45,
    "sell_volume": 120.10,
    "total_volume": 243.55
  }
}
```

Metrics `metric`:
```
{
  "type": "metric",
  "exchange": "binance",
  "symbol": "BTCUSDC,ETHUSDC",
  "ts_ms": 1700000000123,
  "data": {
    "metric": "correlation",
    "symbols": ["BTCUSDC", "ETHUSDC"],
    "interval": "1m",
    "window_ms": 2592000000,
    "value": 0.81
  }
}
```

Full schema examples live in `docs/ws_relay.md`.

## Local run (docker compose)

```
docker compose up --build -d
```

Ports:
- Relay: `8765`
- Metrics: `8766`
