# Recorder Design

This document explains the logic behind `mm_recorder`, the design intentions behind each artifact it writes, and the invariants downstream code is expected to rely on.

Use this document when changing recorder behavior or debugging recorder-side data issues.

For the file-by-file day directory contract, see [docs/data_collection_and_replay.md](../docs/data_collection_and_replay.md).
For exchange-specific sync and checksum rules, see [docs/sync_and_checksum.md](../docs/sync_and_checksum.md).

## Purpose

The recorder exists to turn unstable live exchange websocket traffic into a replayable day dataset with explicit audit boundaries.

That implies four design goals:

1. Preserve enough raw information to rebuild the book deterministically.
2. Preserve enough control-plane information to explain reconnects, resyncs, and failures after the fact.
3. Keep day folders append-friendly and operationally simple.
4. Make downstream replay rely on explicit boundaries, not implicit assumptions about process lifetime.

The recorder is not trying to be a generic event bus or a perfect archival mirror of every raw websocket frame. It stores the subset of information needed for deterministic replay, auditing, trade analysis, and live relay support.

## Core Model

One recorder process handles exactly one `exchange + symbol`.

At runtime, the recorder has two planes:

- Control plane: lifecycle events, resyncs, reconnects, phase changes, and fatal conditions.
- Data plane: snapshots, depth diffs, trades, and derived top-of-book/top-N rows.

The recorder writes both because a correct historical dataset needs more than raw messages:

- snapshots and diffs tell you how to rebuild the book
- events tell you when a previous book became invalid
- gaps tell you why a resync happened
- trades let you align fills against the same ingest timeline

## Why The Day Folder Exists

The recorder groups output by `data/<exchange>/<symbol_fs>/<YYYYMMDD>/`, where `YYYYMMDD` is the configured window start date in the configured timezone.

This is intentional:

- the trading window is an operational boundary
- downstream research typically wants one logical day bundle
- keeping one day directory per symbol keeps schema and replay discovery simple

Within a day folder, artifact files are append-only across restarts. A restart does not start a new day folder by itself.

That design only works safely because:

- `run_id` distinguishes one recorder process run from another
- `recv_seq` is restored from existing day outputs on startup, so ordering remains monotonic across restarts within the same day folder

## Why The Recorder Writes Multiple Views Of The Same Market

The recorder stores multiple representations on purpose. They solve different problems.

### Control-plane ledger

`events_*.csv.gz` is the authoritative timeline.

Intent:

- make replay segment boundaries explicit
- record which snapshot belongs to which segment
- record why the recorder changed phase or stopped trusting the book

Without `events`, downstream code would need to infer validity boundaries from raw data patterns, which is much less reliable.

### Incremental book stream

`diffs/depth_diffs_*.ndjson.gz` is the canonical incremental replay input.

Intent:

- preserve the parsed exchange depth stream with recorder metadata
- provide a stable replay input that can be merged by `recv_seq`
- keep enough exchange-native fields for sequence or checksum validation

This is the source of truth for book updates between snapshots.

### Snapshot artifacts

The recorder writes both normalized CSV snapshots and raw JSON snapshots.

Intent:

- CSV snapshots are easy to inspect and are suitable seeds for sequence-based replay
- raw JSON snapshots preserve exact exchange payloads when checksum logic depends on original string values

This is why checksum exchanges should prefer raw JSON when exact formatting matters.

### Derived top-N book rows

`orderbook_ws_depth_*.csv.gz` exists for research convenience, not authoritative reconstruction.

Intent:

- provide a clean, already-applied top-N series for plots, analytics, and parity checks
- avoid making every downstream notebook re-apply diffs just to inspect the visible book

It is deliberately treated as derived output because it only exists while the local book is known to be valid.

### Normalized and raw trades

The recorder writes both `trades_ws_*.csv.gz` and `trades/trades_ws_raw_*.ndjson.gz`.

Intent:

- normalized trade CSV is the default analysis surface
- raw trade NDJSON keeps exchange-specific fields available for debugging or later schema expansion

Both live on the same ingest timeline through `recv_seq`.

### Live rolling files

`live/live_depth_diffs.ndjson` and `live/live_trades.ndjson` are operational relay buffers.

Intent:

- support low-latency tailing for the websocket relay
- avoid making the relay tail gzip files

These are explicitly not the canonical historical dataset. They exist for operational streaming, not for archival replay.

## Recorder Lifecycle

At a high level, the recorder does this:

1. Resolve exchange adapter and symbol normalization.
2. Compute the current recording window and day directory.
3. Resolve metadata such as tick size and instrument information.
4. Open append writers for the day bundle and rewrite `schema.json`.
5. Restore the last seen `recv_seq` from existing day files.
6. Connect websocket transport.
7. Acquire an initial snapshot.
8. Buffer or apply depth updates until the book becomes valid.
9. While synced, write diffs, trades, events, and derived top-N rows.
10. On gaps, checksum failures, or reconnect boundaries, resync and start a new epoch.
11. At window end or shutdown, emit explicit stop events and close writers.

The key point is that replay validity is segment-based, not process-based.

## State And IDs

Several ids exist because they answer different questions.

### `run_id`

`run_id` identifies one recorder process run.

Intent:

- distinguish same-day restarts within one append-only day folder
- support attribution and forensic repair across process runs

It is not a sequencing key. It is a run boundary marker.

### `recv_seq`

`recv_seq` is the global recorder ingest sequence across the day dataset.

Intent:

- give every emitted depth row, trade row, event row, and gap row a single ordering axis
- allow downstream code to merge heterogeneous artifacts by one monotonic key

Important semantics:

- it is global across artifact types
- it is monotonic, but not contiguous within any single file
- it is an ordering key, not a websocket-native sequence id

### `event_id`

`event_id` identifies rows in the event ledger and also anchors snapshot filenames.

Intent:

- tie snapshot files to explicit ledger events
- make snapshot references stable inside `events.details_json`

### `epoch_id`

`epoch_id` identifies the currently valid local book epoch.

Intent:

- mark that a previous book became invalid after a resync
- let downstream tools reason about synced intervals without guessing

When `epoch_id` changes, consumers should assume the previous incremental book state cannot be continued through that boundary.

## Why Replay Must Read Events

A common mistake is to think replay can just load the first snapshot and apply all diffs in order.

That is wrong because reconnects, gaps, checksum mismatches, and resyncs invalidate earlier book state. The recorder therefore emits explicit `events` rows such as:

- `snapshot_loaded`
- `resync_start`
- `resync_done`
- `state_change`
- `fatal`
- `window_end`

The design intention is:

- `events` define control-plane truth
- `snapshots` and `diffs` define data-plane truth

Replay needs both.

## Why Gaps Exist Separately From Events

`gaps_*.csv.gz` overlaps with `events`, but it is still intentional.

`events` is the authoritative lifecycle ledger.
`gaps` is the focused audit log for integrity problems and resync actions.

That separation exists because integrity investigations often want a compact stream of only exceptional conditions without reading the full lifecycle ledger.

## Exchange Abstraction

The recorder normalizes transport and storage, but not by pretending all exchanges are identical.

The adapter layer isolates:

- symbol normalization
- websocket subscription messages
- snapshot format
- trade parsing
- depth parsing
- sync mode

Today there are two main sync styles:

- sequence-based sync, where bridge and continuity rules matter
- checksum-based sync, where exact book state validation matters

The recorder intentionally keeps exchange-specific fields in raw payloads and diff metadata so replay can still apply exchange-correct logic later.

## Append Semantics And Restart Semantics

The recorder is append-oriented inside a day folder.

Intent:

- avoid creating a new file tree for every operational restart
- keep one logical day dataset
- allow long-running collection windows with occasional restarts

That choice creates strict requirements:

- startup must restore `recv_seq` from existing day outputs before appending
- every row that may need cross-run attribution should carry `run_id`
- downstream tools must treat `events` as the source of run and resync boundaries

This is why `recv_seq` restart safety and `run_id` propagation are not optional details. They are part of the append-within-a-day design contract.

## What Is Canonical And What Is Not

Canonical historical replay inputs:

- `schema.json`
- `events_*.csv.gz`
- `snapshots/`
- `diffs/depth_diffs_*.ndjson.gz`

Canonical but analysis-oriented:

- `trades_ws_*.csv.gz`

Derived or diagnostic:

- `orderbook_ws_depth_*.csv.gz`
- `gaps_*.csv.gz`
- `trades/trades_ws_raw_*.ndjson.gz`

Operational only:

- `live/*.ndjson`

This distinction matters because many recorder bugs come from treating a convenience artifact as if it were the source of truth.

## Why The Recorder Rewrites `schema.json`

`schema.json` is overwritten on each recorder start to reflect the current declared layout for that day folder.

Intent:

- make the file layout explicit instead of forcing downstream code to hardcode names
- allow controlled schema evolution while keeping historical folders loadable

The schema is a manifest, not an audit log. It describes the current contract of the day directory, not every change that happened over time.

## Operational Tradeoffs

Some design choices are deliberate tradeoffs:

- Append-in-place day files are simpler operationally than per-run trees, but require explicit `run_id` and restart-safe sequencing.
- Writing both normalized and raw views costs extra storage, but prevents downstream schema lock-in.
- Writing derived top-N rows increases storage, but removes a lot of duplicated replay work in research code.
- Keeping live rolling files separate avoids coupling relay behavior to canonical gzip outputs.

These tradeoffs are acceptable because the recorder prioritizes replay correctness and debuggability over minimal byte usage.

## Guidance For Future Changes

When changing recorder behavior, keep these questions explicit:

1. Is this artifact canonical, derived, diagnostic, or operational?
2. Does the change affect replay correctness, or only convenience outputs?
3. Does it preserve monotonic `recv_seq` and correct `run_id` attribution across same-day restarts?
4. Does it preserve the meaning of `events` as the control-plane ledger?
5. Does it change exchange-specific replay requirements?

If a proposed change cannot answer those questions clearly, the design is probably still ambiguous.
