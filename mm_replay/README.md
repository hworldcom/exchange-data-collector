# mm_replay

Deterministic replay engine for recorded market data produced by
`mm_recorder`.

------------------------------------------------------------------------

## Overview

`mm_replay` is a standalone service and library that replays previously
recorded exchange market data in a fully deterministic, event-driven
manner. It is designed specifically to operate on datasets produced by
our internal `mm_recorder` system and does **not** support third-party
or external data formats.

The primary goals of `mm_replay` are:

-   Deterministic reconstruction of local order books
-   Strategy backtesting on recorded market microstructure data
-   Data integrity validation (sequence / checksum correctness)
-   Reproducible research and debugging of production behavior

`mm_replay` intentionally has no dependency on live APIs, websocket
clients, or historical REST fetchers.

------------------------------------------------------------------------

## Scope

`mm_replay` supports:

-   Binance (sequence-based order book synchronization)
-   Kraken (CRC32 checksum-based validation)
-   Bitfinex (CRC32 checksum-based validation)
-   Multi-segment sessions (resync-aware)
-   Deterministic processing using recorded `recv_seq`

`mm_replay` does not:

-   Connect to exchanges
-   Fetch historical snapshots
-   Normalize third-party datasets
-   Modify recorded artifacts

It is a pure replay and validation engine.

------------------------------------------------------------------------

## Architectural Principles

### 1. Deterministic Ordering

Replay ordering is driven exclusively by `recv_seq` (receive sequence
ID) recorded at ingestion time.

-   No reliance on exchange timestamps
-   No reliance on filesystem ordering
-   No reliance on wall-clock time

This guarantees identical results across machines and runs.

------------------------------------------------------------------------

### 2. Exact Logic Reuse

Replay uses the same synchronization engines and order book logic as
production:

-   `LocalOrderBook`
-   Binance sequence-based sync engine
-   Kraken checksum engine
-   Bitfinex checksum engine

This ensures replay behavior is bit-for-bit consistent with live
recording logic.

------------------------------------------------------------------------

### 3. Event-Driven Execution

Replay is structured as an event loop over recorded artifacts:

-   Snapshot load events
-   Depth diff updates
-   Trade events
-   Resync markers
-   Gap detection markers

Strategy callbacks are triggered only when book state is valid.

------------------------------------------------------------------------

### 4. Explicit Validity State

The engine maintains book validity:

-   `VALID` after snapshot load + successful bridging
-   `INVALID` on detected gaps or resync start
-   Revalidated after successful snapshot adoption

Strategies are never invoked while book state is invalid.

------------------------------------------------------------------------

## Supported Dataset Format

`mm_replay` operates exclusively on datasets produced by `mm_recorder`.

A dataset consists of:

### 1. Events Ledger (gzip CSV)

Contains:

-   `event_id`
-   `recv_time_ms`
-   `recv_seq`
-   `run_id`
-   `type`
-   `epoch_id`
-   `details_json`

This is the authoritative timeline.

------------------------------------------------------------------------

### 2. Depth Diffs (gzip NDJSON)

Each record contains:

-   `recv_seq`
-   Exchange-specific payload fields
-   Raw price/quantity strings (precision preserved)

Diff files are strictly monotonic in `recv_seq`, but may not be contiguous.
`recv_seq` is assigned from a global ingest timeline (depth/trade/events/gaps), so
other artifacts can consume sequence ids between adjacent diff rows.

------------------------------------------------------------------------

### 3. Snapshots

Snapshot files referenced by `snapshot_loaded` events.

Used to initialize order book state at the start of each valid segment.

------------------------------------------------------------------------

## Replay Execution Model

### Step 1 --- Load Dataset

-   Parse schema and manifest
-   Load events ledger
-   Build replay segments based on:
    -   `snapshot_loaded`
    -   `resync_start`
    -   gap markers

------------------------------------------------------------------------

### Step 2 --- Segment Initialization

For each segment:

1.  Load snapshot
2.  Initialize appropriate sync engine
3.  Set book validity to `VALID`

------------------------------------------------------------------------

### Step 3 --- Stream Diffs

For each diff with `recv_seq > snapshot_recv_seq`:

-   Feed into sync engine
-   If gap detected:
    -   Mark book invalid
    -   End segment
-   If successful:
    -   Emit `on_book_update`

------------------------------------------------------------------------

### Step 4 --- Strategy Callbacks

Replay engine supports strategy interfaces:

```python
class Strategy:
    def on_segment_start(self, segment): ...
    def on_book_update(self, recv_seq, book): ...
    def on_trade(self, recv_seq, trade): ...
    def on_gap(self, recv_seq): ...
```

Strategies are executed in strict `recv_seq` order.

------------------------------------------------------------------------

## Exchange-Specific Validation

### Binance

-   Snapshot `lastUpdateId` bridging enforced
-   Sequence continuity enforced
-   Gap -> resync required

------------------------------------------------------------------------

### Kraken

-   Depth truncation to subscribed level enforced
-   CRC32 checksum validated
-   Mismatch -> invalid segment

------------------------------------------------------------------------

### Bitfinex

-   Snapshot + incremental updates
-   CRC32 checksum (top 25 levels)
-   Mismatch -> resubscribe required

------------------------------------------------------------------------

## Determinism Guarantees

Replay guarantees:

-   Identical order book state given identical dataset
-   No dependency on system clock
-   No race conditions
-   No concurrency
-   Single-threaded, deterministic execution

------------------------------------------------------------------------

## Validation Mode

`mm_replay` includes a validation mode that:

-   Reconstructs books per segment
-   Verifies sequence continuity
-   Verifies checksum correctness
-   Detects unhandled gaps
-   Reports per-segment integrity metrics

This is used to assert dataset correctness before strategy backtesting.

------------------------------------------------------------------------

## Intended Users

-   Quantitative researchers
-   Strategy developers
-   Infrastructure engineers
-   Data integrity reviewers

------------------------------------------------------------------------

## Non-Goals

-   Real-time simulation
-   Order matching engine simulation
-   Market impact modeling
-   Latency modeling

`mm_replay` is a market data replay engine, not a full exchange
simulator.

------------------------------------------------------------------------

## Versioning and Compatibility

`mm_replay` is compatible only with datasets produced by supported
versions of `mm_recorder`.

Dataset schema version must match supported schema range. Incompatible
schema versions will result in explicit startup failure.

------------------------------------------------------------------------

## Future Extensions (Optional)

-   Multi-symbol replay
-   Multi-venue synchronized replay
-   Feature extraction pipeline
-   Deterministic export of top-of-book time series
-   Performance benchmarking mode

------------------------------------------------------------------------

## Summary

`mm_replay` provides a deterministic, production-grade replay engine for
internally recorded exchange market data. It guarantees consistency with
live synchronization logic and enables reproducible research, strategy
backtesting, and data validation without any dependency on live
infrastructure.

It is intentionally narrow in scope, focused, and tightly integrated
with our internal dataset format to ensure correctness and operational
reliability.
