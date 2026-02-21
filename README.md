# Market Data Module

This package houses every component required to capture Binance Spot depth and trade data, reconstruct level-2 books, and persist replay-ready artifacts for downstream research.

It includes the `mm_core` package for the shared order book and sync state machine.
The recorder avoids `python-binance` and uses a lightweight Binance REST client with
retries for snapshots; if you later need signed endpoints or user streams, you can
reintroduce them via a custom client.

## Architecture

| File | Responsibility |
|------|----------------|
| `mm_recorder/recorder.py` | End-to-end orchestration: enforces the Berlin trading window, wires callbacks, persists CSV/NDJSON outputs, and emits telemetry/events. |
| `mm_core/sync_engine.py` | Pure state machine that bridges REST snapshots with WebSocket depth diffs and detects any sequencing gap. |
| `mm_core/checksum/kraken.py` | Checksum-based sync engine for exchanges like Kraken (verifies CRC checksums instead of sequence ids). |
| `mm_core/checksum/bitfinex.py` | Checksum-based sync engine for Bitfinex (CRC checksum frames). |
| `mm_core/local_orderbook.py` | SortedDict-backed in-memory book keyed by price, used by the recorder and sync engine. |
| `mm_recorder/recorder_callbacks.py` | WebSocket callback handlers (emit, heartbeat, snapshot, depth, trade). |
| `mm_recorder/recorder_context.py` | Recorder wiring for writers, adapters, and runtime state. |
| `mm_recorder/recorder_settings.py` | Env-backed configuration constants. |
| `mm_recorder/recorder_types.py` | Recorder phase and state dataclasses. |
| `mm_recorder/buffered_writer.py` | Buffered CSV writer that batches rows in memory to reduce fsync pressure. |
| `mm_recorder/ws_stream.py` | Async websocket client with reconnect, ping/pong, and backoff. |
| `mm_recorder/snapshot.py` | Snapshot helper that serializes CSV + raw JSON snapshots for audit and resyncs. |

The repository includes tests that validate epochs, header handling, and recorder output contracts. Those tests run offline by monkeypatching `record_rest_snapshot` while the recorder uses the lightweight REST client in normal runs.

Recent additions:
- `mm_history` for historical candle/trade extraction and local+exchange candle combining.
- `mm_api` for a lightweight WS relay and minimal REST snapshot endpoint.
- Rolling `live/` files for low-latency relay tailing.

## Output contract

Each recorder run (one symbol per process) produces the following files under `data/<EXCHANGE>/<SYMBOL_FS>/<YYYYMMDD>/`:

- `orderbook_ws_depth_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — top-N book frames whenever the local book is synced.
- `trades_ws_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — individual trade prints with event/receive timestamps and trade identifiers.
- `events_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — authoritative ledger covering WS lifecycle, snapshot tags, resync epochs, and run boundaries.
- `gaps_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — optional audit of detected sequencing issues.
- `snapshots/snapshot_<event_id>_<tag>.csv` — REST snapshots referenced by the events ledger.
- `snapshots/snapshot_<event_id>_<tag>.json` — raw snapshot payload (REST for Binance, WS for checksum exchanges).
- `diffs/depth_diffs_<SYMBOL_FS>_<YYYYMMDD>.ndjson.gz` — optional compressed raw WS diffs for exact replays (checksum exchanges include a `checksum` field per diff).
- `trades/trades_ws_raw_<SYMBOL_FS>_<YYYYMMDD>.ndjson.gz` — raw trade payloads with recv sequence metadata.
- `live/live_depth_diffs.ndjson` — rolling uncompressed live diffs for WS relay (rotated + retained).
- `live/live_trades.ndjson` — rolling uncompressed live trades for WS relay (rotated + retained).

Uncompressed outputs are intentionally not supported. Avoid renaming columns or folders unless you also update downstream consumers.

## Running the recorder

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Export the target symbol (case-insensitive):
   ```bash
   export SYMBOL=ETHUSDT
   ```
   Optional exchange (defaults to Binance):
   ```bash
   export EXCHANGE=binance  # or kraken
   ```
3. Launch the recorder:
   ```bash
   python -m mm_recorder.recorder
   ```

### Local run notes

- Logs are written to `logs/recorder/<EXCHANGE>/<SYMBOL_FS>/<YYYY-MM-DD>.log` where `SYMBOL_FS` strips `/ - :` and spaces.
- If your local TLS inspection blocks the websocket handshake, set `INSECURE_TLS=1`
  (only for local debugging).
- Kraken adapter uses WebSocket v2 `book` channel with checksum-based sync and subscribes to `trade` for fills.

## Exchange data formats

### Binance (spot)
- **Order book diffs**: `diffs/depth_diffs_*.ndjson.gz` uses Binance `depthUpdate` fields (`E`, `U`, `u`, `b`, `a`) and includes `raw` for the full payload.
- **Trades**: `trades_ws_*.csv.gz` captures standard Binance trade fields plus `side` (derived from maker flag), with raw payloads stored in `trades/trades_ws_raw_*.ndjson.gz`.
- **Snapshots**: REST snapshot saved to CSV plus raw JSON (`snapshot_*.json`).

### Kraken (spot, WS v2)
- **Order book diffs**: checksum-driven; `U/u` are `0` because Kraken doesn’t provide them. `checksum`, `exchange`, `symbol`, and `raw` are persisted in the diff NDJSON for replay verification.
- **Trades**: `trade` channel parsed into the shared trade schema with `side` and `ord_type` when present; raw payloads stored in `trades/trades_ws_raw_*.ndjson.gz`.
- **Snapshots**: WS snapshot saved to CSV plus raw JSON (`snapshot_*.json`), with checksum stored in both the snapshot CSV and events ledger.

### Bitfinex (spot, WS v2)
- **Order book diffs**: checksum-driven (book + checksum frames). Depth normalized to 25. Updates are applied per price/count/amount; checksum frames validate state.
- **Trades**: `trades` channel parsed into shared trade schema with `side` derived from amount sign; raw payloads stored in `trades/trades_ws_raw_*.ndjson.gz`.

Note: raw JSON payloads may contain Decimal values serialized as strings to preserve precision for checksum verification.

See `docs/sync_and_checksum.md` for detailed sync/checksum logic and known issues per exchange.
See `docs/ws_relay.md` for the WebSocket relay that streams live data to a frontend.

## Recorder state machine

The recorder tracks an explicit state machine and emits `state_change` events:

`CONNECTING → SNAPSHOT → SYNCING → SYNCED`  
Any gap or checksum mismatch transitions to `RESYNCING`, and on shutdown to `STOPPED`.

## Replay notes

To rebuild the order book for a day:

1. Load the first snapshot CSV (or JSON) for the session.
2. Apply each diff in `diffs/depth_diffs_*.ndjson.gz` in order of `recv_seq`.
3. For Binance, validate sequential `U/u` ranges. For Kraken, validate the per-diff `checksum` after applying each update.
4. If a gap is detected, jump to the next snapshot tagged in `events_*.csv.gz` (look for `resync_start`/`resync_done`).

Replay should ignore diffs received before the initial snapshot is loaded. The `events_*.csv.gz` ledger provides the authoritative timeline (window boundaries, reconnects, resync tags).

### Replay validator

An offline validator is included to replay recorded diffs against snapshots and confirm reconciliation:

```bash
python -m mm_recorder.replay_validator --day-dir data/<EXCHANGE>/<SYMBOL_FS>/<YYYYMMDD>
```

It returns exit code `0` if no gaps are detected, and `1` if any gap/checksum mismatch is found.

## Historical data utilities

### Fetch candles/trades
```bash
EXCHANGE=binance SYMBOL=BTCUSDT TYPE=candles INTERVAL=1m START_MS=1709337600000 END_MS=1709341200000 \
python -m mm_history.cli

EXCHANGE=binance SYMBOL=BTCUSDT TYPE=trades START_MS=1709337600000 END_MS=1709341200000 \
python -m mm_history.cli
```

### Combine local + exchange candles
The combiner prefers local candles and logs an error if overlap candles differ from the exchange:
```bash
python -m mm_history.smoke
```

### Gap smoke test (local vs exchange)
```bash
python -m mm_history.smoke_gap
```

## Tests

```bash
python3 -m pytest -q
```

### Docker

```
docker build -t mm-recorder .
docker run --rm \
  -e SYMBOL=ETHUSDT \
  -v "$PWD/data":/app/data \
  mm-recorder
```

### Docker Compose (multi-service)

`docker-compose.yml` starts:
- 4 Binance recorders: BTCUSDC, ETHUSDC, SOLUSDC, XRPUSDC
- WS relay (port 8765)
- Metrics WS (port 8766)

Run:
```bash
docker compose up --build
```

Detached:
```bash
docker compose up --build -d
```

Stop:
```bash
docker compose down
```

If you vendor this repo into another build context, ensure `mm_core` is present alongside `mm_recorder`.

### Configuration knobs

| Variable/Const | Meaning |
|----------------|---------|
| `EXCHANGE` (env) | Exchange adapter to use (default: `binance`). Supported: `binance`, `kraken`, `bitfinex`. |
| `SYMBOL` (env) | Trading pair to subscribe (e.g., `BTCUSDT`). Required. |
| `DEPTH_LEVELS` | Number of L2 levels persisted per book snapshot row. |
| `STORE_DEPTH_DIFFS` | Toggle gzip’d NDJSON logging of raw WS depth diffs for replay. |
| `WS_PING_INTERVAL_S`, `WS_PING_TIMEOUT_S` | Client ping cadence and pong timeout (seconds). |
| `WS_RECONNECT_BACKOFF_S`, `WS_RECONNECT_BACKOFF_MAX_S` | Reconnect backoff base and cap (seconds). |
| `WS_MAX_SESSION_S` | Max WS session duration before forced reconnect (seconds). |
| `WS_OPEN_TIMEOUT_S` | WebSocket handshake/open timeout (seconds). |
| `WS_NO_DATA_WARN_S` | Warn if no WS messages are received for this many seconds. |
| `LIVE_STREAM` | Enable rolling uncompressed live files for WS relay (default: `1`). |
| `LIVE_STREAM_ROTATE_S` | Rotate live files after this many seconds (default: `60`). |
| `LIVE_STREAM_RETENTION_S` | Retain rotated live files for this many seconds (default: `3600`). |
| `BINANCE_REST_BASE_URL` | Base URL for Binance REST snapshots (default: `https://api.binance.com`). |
| `KRAKEN_REST_BASE_URL` | Base URL for Kraken REST metadata (default: `https://api.kraken.com`). |
| `BITFINEX_REST_BASE_URL` | Base URL for Bitfinex REST metadata (default: `https://api.bitfinex.com`). |
| `SNAPSHOT_TIMEOUT_S` | REST snapshot timeout in seconds (default: `10`). |
| `SNAPSHOT_RETRY_MAX` | Max snapshot retries (default: `3`). |
| `SNAPSHOT_RETRY_BACKOFF_S` | Initial snapshot retry backoff in seconds (default: `0.5`). |
| `SNAPSHOT_RETRY_BACKOFF_MAX_S` | Max snapshot retry backoff in seconds (default: `5`). |
| `MM_METADATA_FETCH` | Fetch exchange metadata at startup for price tick sizes (default: `1`). If `0`, you must set `MM_PRICE_TICK_SIZE`. |
| `MM_METADATA_STRICT` | Fail if metadata fetch fails (default: `1`). |
| `MM_PRICE_TICK_SIZE` | Override price tick size (skip REST metadata) when set. |
| `METADATA_TIMEOUT_S` | Metadata REST timeout in seconds (default: `10`). |
| `METADATA_RETRY_MAX` | Max metadata fetch retries (default: `3`). |
| `METADATA_RETRY_BACKOFF_S` | Initial metadata retry backoff in seconds (default: `0.5`). |
| `METADATA_RETRY_BACKOFF_MAX_S` | Max metadata retry backoff in seconds (default: `5`). |

Note: Bitfinex does not publish a fixed tick size; the recorder derives a proxy from `price_precision` (significant digits) and normalizes book prices to those significant digits when available.
| `WINDOW_TZ` (env) | Timezone used for start/end windows (default: `Europe/Berlin`). |
| `WINDOW_START_HHMM` (env) | Window start time in 24h `HH:MM` (default: `00:00`). |
| `WINDOW_END_HHMM` (env) | Window end time in 24h `HH:MM` (default: `00:15`). |
| `WINDOW_END_DAY_OFFSET` (env) | Day offset added to the end time (default: `1`). Use `1` for next-day cutoff. |
| `HEARTBEAT_SEC`, `SYNC_WARN_AFTER_SEC`, `MAX_BUFFER_WARN` | Telemetry cadence and warning thresholds. |
| `ORDERBOOK_BUFFER_ROWS`, `TRADES_BUFFER_ROWS`, `BUFFER_FLUSH_INTERVAL_SEC` | Tune throughput vs. fsync pressure. |

## WebSocket relay + REST snapshot

Start the live relay:
```bash
python -m mm_api.relay
```

Start the minimal REST snapshot endpoint:
```bash
python -m mm_api.rest
```

See `docs/ws_relay.md` for the full message format and connection details.
See `docs/local_client_setup.md` for local client setup and example commands.
See `docs/client_connection_guide.md` for a client-facing connection guide.

### Connect to streamers (quick)

Relay WS:
```
ws://localhost:8765/ws?exchange=binance&symbol=BTCUSDC
```

Metrics WS:
```
ws://localhost:8766/metrics?exchange=binance&symbols=BTCUSDC,ETHUSDC&interval=1m&window=30d&metric=correlation
```

### WS clients (manual)
```
python ws_clients/relay_client.py
python ws_clients/metrics_client.py
```

### Dependencies and testing notes

- `mm_recorder.logging_config` is used to configure per-run logging.
- `mm_core` supplies the shared order book and sync engine.
- `sortedcontainers` is used for an efficient in-memory order book.
- Tests under `tests/` include checksum engine coverage for Kraken/Bitfinex and monkeypatch `record_rest_snapshot` for offline runs.
