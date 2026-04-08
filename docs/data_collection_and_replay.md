# Data Collection And Replay

This document describes how this project collects market data, what it writes to disk, and how a downstream research project should replay that data correctly.

It is intended to be the practical dataset contract for consumers of `data/<exchange>/<symbol>/<YYYYMMDD>/`.

## Purpose

The recorder captures live exchange market data and persists enough information to:

- replay the order book deterministically
- validate sequence or checksum integrity
- audit gaps, reconnects, and resyncs
- analyze trades and book states offline

The key idea is:

- `events_*.csv.gz` is the authoritative timeline
- `snapshots/` and `diffs/` are the authoritative replay inputs
- `orderbook_ws_depth_*.csv.gz` is a derived artifact written by the recorder after sync, not the source of truth for reconstruction

## Collection Lifecycle

Each recorder process handles one `exchange + symbol`.

At startup it:

1. Resolves the exchange adapter and symbol format.
2. Computes the recording window and day directory.
3. Creates output writers and writes `schema.json`.
4. Connects to the exchange websocket.
5. Loads an initial snapshot.
6. Buffers or applies depth updates until the book is valid.
7. Writes trades, diffs, events, and synced top-N book rows.
8. Resyncs on detected gaps or checksum failures.
9. Stops at `window_end` and emits shutdown events.

The exact sync behavior depends on the exchange:

- Binance: sequence-based sync using `lastUpdateId`, `U`, and `u`
- Kraken: checksum-based sync using WS snapshot/update checksums
- Bitfinex: checksum-based sync using checksum frames

## Directory Layout

Each run writes under:

```text
data/<exchange>/<symbol_fs>/<YYYYMMDD>/
```

Example:

```text
data/binance/BTCUSDT/20260221/
data/kraken/BTCUSDC/20260222/
```

`YYYYMMDD` is based on the configured recording window start date, not necessarily UTC midnight.

## Output Files

### `schema.json`

Small manifest describing the dataset version and file layout.

Use it first when loading a day directory.

Important fields:

- `schema_version`
- `instrument.base_asset`
- `instrument.quote_asset`
- `instrument.asset_source`
- `files.events_csv.path`
- `files.depth_diffs_ndjson_gz.path`
- `files.trades_ws_csv.path`
- `files.orderbook_ws_depth_csv.path`
- `files.depth_diffs_ndjson_gz.depth` for checksum exchanges

Consumers should tolerate schema versions `2`, `3`, and `4`, because historical data in this repository already includes older versions.
Newer datasets may use schema version `5`, which adds the optional `instrument` block.

The optional `instrument` block stores day-level symbol metadata such as:

- `exchange`
- `symbol`
- `base_asset`
- `quote_asset`
- `asset_source`
- `tick_size`
- `tick_size_source`

Asset source semantics:

- Binance and Kraken use exchange metadata APIs
- Bitfinex derives base/quote from symbol parsing rules

### `events_<symbol>_<day>.csv.gz`

The authoritative ledger.

Columns:

- `event_id`
- `recv_time_ms`
- `recv_seq`
- `run_id`
- `type`
- `epoch_id`
- `details_json`

This file records:

- run start/stop
- websocket connect/reconnect lifecycle
- snapshot requests and snapshot loads
- state transitions
- resync boundaries
- fatal conditions
- window end

Replay must use this file to decide where segments start and end.

Important event types:

- `snapshot_loaded`
- `resync_start`
- `resync_done`
- `window_end`
- `fatal`
- `state_change`

For checksum exchanges, `snapshot_loaded.details_json` may include:

- `checksum`
- `raw_path`

For Kraken replay, `raw_path` is especially important because checksum validation depends on the original string representation from the exchange snapshot.

### `gaps_<symbol>_<day>.csv.gz`

Audit log of detected gaps and resync operations.

Columns:

- `recv_time_ms`
- `recv_seq`
- `run_id`
- `epoch_id`
- `event`
- `details`

Typical rows:

- `resync_start`
- `resync_done`
- `fatal`

This is useful for diagnostics, but replay should still treat `events_*.csv.gz` as the primary source of segment boundaries.

### `snapshots/snapshot_<event_id>_<tag>.csv`

Normalized snapshot written at initial sync or resync.

Columns:

- `run_id`
- `event_id`
- `side`
- `price`
- `qty`
- `lastUpdateId`
- optional `checksum`

Usage:

- Binance replay seeds from this CSV snapshot.
- It is also useful as a generic book snapshot for inspection.

Caveat:

- For checksum exchanges, the CSV snapshot may normalize numeric formatting.
- That is acceptable for inspection and for top-N book reconstruction, but it can be wrong for checksum seeding if the exchange checksum depends on exact string formatting.

### `snapshots/snapshot_<event_id>_<tag>.json`

Raw snapshot payload from the exchange.

Usage:

- Binance: raw REST payload for audit
- Kraken: raw WS snapshot payload and checksum source
- Bitfinex: raw WS snapshot payload

For Kraken, replay should prefer this raw JSON over the CSV snapshot when seeding checksum state, because Kraken checksum calculation depends on the exact exchange-provided string values.

### `diffs/depth_diffs_<symbol>_<day>.ndjson.gz`

Authoritative depth update stream for replay.

Each line is one parsed depth diff plus recorder metadata.

Common fields:

- `recv_ms`
- `recv_seq`
- `run_id`
- `E`
- `U`
- `u`
- `b`
- `a`
- `exchange`
- `symbol`
- optional `checksum`
- optional `raw`

Exchange-specific meaning:

- Binance:
  - `U` and `u` are real sequence ids
  - replay must enforce bridge and continuity rules
- Kraken:
  - `U` and `u` are `0`
  - `checksum` is the integrity signal
- Bitfinex:
  - `U` and `u` are `0`
  - `checksum` or checksum frames in `raw` drive validation

This file, not `orderbook_ws_depth`, is the canonical incremental order book stream.

### `orderbook_ws_depth_<symbol>_<day>.csv.gz`

Derived top-N order book rows written by the recorder only while the book is valid and synced.

Columns begin with:

- `event_time_ms`
- `recv_time_ms`
- `recv_seq`
- `run_id`
- `epoch_id`

Then the flattened levels:

- `bid1_price`, `bid1_qty`, `ask1_price`, `ask1_qty`
- ...

Meaning:

- this is the recorder's sampled top-N view after each applied depth message
- it is useful for research, plotting, and parity checking
- it is not the source of truth for reconstruction

Do not replay from this file alone if you need a correct, segment-aware book.

### `trades_ws_<symbol>_<day>.csv.gz`

Normalized trade stream.

Columns typically include:

- `event_time_ms`
- `recv_time_ms`
- `recv_seq`
- `run_id`
- `trade_id`
- `trade_time_ms`
- `price`
- `qty`
- `is_buyer_maker`
- optional newer fields: `side`, `ord_type`, `exchange`, `symbol`

Use this file for trade analysis and, if desired, interleave trade events into replay output by `recv_seq`.

### `trades/trades_ws_raw_<symbol>_<day>.ndjson.gz`

Raw trade payloads with recorder metadata.

Common fields include:

- `recv_ms`
- `recv_seq`
- `run_id`
- `event_time_ms`
- `trade_id`
- `exchange`
- `symbol`
- optional `raw`

Useful for:

- exchange-specific debugging
- preserving fields not carried into the normalized CSV

### `live/live_depth_diffs.ndjson` and `live/live_trades.ndjson`

Rolling uncompressed files for the websocket relay.

They mirror the corresponding diff/raw-trade NDJSON payloads, including recorder metadata such as `recv_seq` and `run_id`.

These are operational artifacts for live streaming and should not be treated as the canonical historical dataset.

## Meaning Of Core IDs

### `recv_seq`

`recv_seq` is the global recorder ingest sequence.

It is:

- monotonic across the full recorder process
- shared by depth rows, trade rows, event rows, and gap rows
- an ordering key, not a contiguous per-stream counter

Implications:

- adjacent diff rows may have large `recv_seq` jumps
- adjacent trade rows may also have `recv_seq` gaps
- those gaps do not imply missing depth or missing trades

Research and replay code should sort and merge by `recv_seq`, not expect contiguity.

### `run_id`

Identifies one recorder process run. Files are append-only across restarts within a day, so multiple `run_id` values can appear in the same daily output files.

### `epoch_id`

Identifies the current valid book epoch.

It increases on resync. A new `epoch_id` means the previous book state should not be continued through that boundary.

## Correct Replay Contract

Replay should be segment-based, not file-based.

### Source Of Truth

Use:

- `schema.json`
- `events_*.csv.gz`
- `snapshots/`
- `diffs/*.ndjson.gz`

Optionally use:

- `trades_ws_*.csv.gz` for trade interleaving

Do not use:

- `orderbook_ws_depth_*.csv.gz` as the authoritative replay input

### Segment Construction

Build replay segments from `events`.

Start a segment at each `snapshot_loaded`.

End a segment at the earliest of:

- the next `resync_start` after that snapshot
- the next `snapshot_loaded` after that snapshot
- end of file

This is the correct contract because:

- a resync invalidates the old book
- a later snapshot replaces the old seed even if no explicit `resync_start` exists

### Replay Algorithm

For each segment:

1. Load the segment snapshot.
2. Initialize the proper sync engine.
3. Skip diffs with `recv_seq <= snapshot_loaded.recv_seq`.
4. Process diffs in increasing `recv_seq`.
5. Stop at the segment boundary.
6. If trades are requested, emit them interleaved by `recv_seq`.

### Binance Replay

Seed from:

- `snapshots/snapshot_*.csv`

Apply:

- `diffs/depth_diffs_*.ndjson.gz`

Rules:

1. Buffer diffs until snapshot exists.
2. Drop any diff with `u <= lastUpdateId`.
3. Find the first bridge diff where `U <= lastUpdateId + 1 <= u`.
4. Apply that diff.
5. Continue only while diffs are sequential.
6. If `U > last_update_id + 1`, the segment is invalid and replay must stop or emit a discontinuity.

### Kraken Replay

Seed from:

- `snapshots/snapshot_*.json` if available
- otherwise fall back to `snapshot_*.csv`

Apply:

- `diffs/depth_diffs_*.ndjson.gz`

Rules:

1. Preserve the original snapshot string values when seeding checksum state.
2. Apply each diff update.
3. Recompute the Kraken checksum after every diff.
4. If computed checksum differs from recorded checksum, the segment is invalid.
5. Continue only from the next `snapshot_loaded` segment.

Reason for raw JSON preference:

- Kraken checksum depends on exchange-provided string formatting.
- The normalized CSV snapshot may rewrite values like `67433.23` to `67433.23000000`.
- That changes checksum results even when the numeric book appears identical.

### Bitfinex Replay

Seed from:

- current implementation uses the snapshot CSV unless richer raw snapshot handling is added

Apply:

- `diffs/depth_diffs_*.ndjson.gz`

Rules:

- apply updates/checksum frames in order
- validate checksum after checksum-bearing messages
- stop the segment on checksum failure

For future downstream systems, preserving exact raw snapshot/update payloads is recommended for checksum exchanges.

## Trade Replay

If you need a single event stream:

1. Replay book diffs by segment.
2. Load `trades_ws_*.csv.gz`.
3. Emit book and trade frames merged by `recv_seq`.

Important:

- use `recv_seq`, not event timestamps, for deterministic ordering
- drop trades whose `recv_seq` falls inside an invalid segment range if you are enforcing strict segment validity

## What A Research Project Should Use

For correct market reconstruction, a downstream analysis project should read:

- `schema.json`
- `events_*.csv.gz`
- `snapshots/*.csv`
- `snapshots/*.json` for checksum exchanges, especially Kraken
- `diffs/*.ndjson.gz`
- `trades_ws_*.csv.gz` if trade replay is needed

It may also read:

- `orderbook_ws_depth_*.csv.gz` for convenience analytics, parity checks, and precomputed top-N studies

Recommended design:

- strict mode: stop on invalid segment/gap/checksum failure
- best-effort mode: emit discontinuity markers and continue from the next valid segment

## Expected Failure Modes

### Truncated gzip files

Example:

- `Compressed file ended before the end-of-stream marker was reached`

Meaning:

- recorder was interrupted
- file was copied before close
- disk or process failure occurred

This is a dataset integrity issue, not a replay algorithm issue.

### Checksum mismatch on first diff of a Kraken segment

Possible causes:

- old replay implementation seeded from normalized CSV instead of raw JSON
- snapshot/diff pair is genuinely inconsistent

### Repeated resync cycles

Meaning:

- the recorder itself observed checksum or sequence failures live
- replay should reproduce those boundaries, not smooth over them

## Summary

The correct replay model for this repository is:

- events define the timeline
- snapshots define segment starts
- diffs define the incremental book
- trades are separate and can be interleaved by `recv_seq`
- `orderbook_ws_depth` is derived output, not the canonical replay source

For Binance:

- replay from snapshot CSV + diffs + events

For Kraken:

- replay from raw snapshot JSON + diffs + events whenever raw JSON is available

For all exchanges:

- respect `epoch_id` and segment boundaries
- treat `recv_seq` as the single deterministic ordering key
