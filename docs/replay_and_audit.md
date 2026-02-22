# Replay And Audit

This project now has two separate validation layers:

- `mm_replay`: validates replay correctness and dataset internal integrity
- `mm_audit`: validates recorded data against external/reference data (or cross-checks internal artifacts)

They answer different questions.

## What `mm_replay` Validates

`mm_replay` reconstructs order books from recorded snapshots + diffs using the same sync engines as the recorder.

It validates:

- file parseability (`schema.json`, events, diffs, snapshots)
- sequence/checksum integrity via sync engines
- resync segment handling
- deterministic orderbook reconstruction

Useful commands:

```bash
# Internal integrity only (no frames emitted)
python3 -m mm_replay \
  --day-dir data/binance/BTCUSDT/20260221 \
  --exchange binance \
  --validate-only \
  --on-error strict
```

```bash
# Replay book + trade frames (NDJSON)
python3 -m mm_replay \
  --day-dir data/binance/BTCUSDT/20260221 \
  --exchange binance \
  --include-trades \
  --top-n 5 \
  --speed 0
```

## Replay Parity (`mm_replay.parity`)

`mm_replay.parity` compares replayed orderbook frames to recorded `orderbook_ws_depth_*.csv.gz`.

It validates replay implementation correctness against recorder-produced top-of-book/top-N rows.

```bash
python3 -m mm_replay.parity \
  --day-dir data/binance/BTCUSDT/20260221 \
  --exchange binance \
  --top-n 1
```

Notes:

- Parity compares by `recv_seq`.
- A known edge case exists near `window_end`: the recorder may write the final depth diff to `diffs` but stop before writing the corresponding `orderbook_ws_depth` row. The parity tool recognizes and ignores this single trailing frame pattern.

## What `mm_audit` Validates

`mm_audit` is for external or cross-artifact checks. It intentionally depends on reference data and may use live historical APIs.

Current tools:

- `mm_audit.binance_candles`
  - Aggregates recorded trades into 1-minute OHLCV
  - Compares against Binance historical 1m klines (`mm_history`)
- `mm_audit.orderbook_trades`
  - Cross-checks recorded trades against recorded top-of-book snapshots
  - Flags crossed books and implausible trade prices relative to nearby top-of-book

### Binance Trade-vs-Historical Candle Audit

```bash
python3 -m mm_audit.binance_candles \
  --day-dir data/binance/BTCUSDT/20260221
```

This is an **approximate external audit** (OHLCV parity), not exact orderbook validation.

Tip: use `--ignore-boundary-partials` to ignore expected startup/shutdown partial-minute artifacts at window boundaries.

## Orderbook-vs-Trade Consistency Audit

This tool uses only recorded artifacts (`orderbook_ws_depth` + `trades_ws`).

Checks:

- crossed top-of-book (`bid1 > ask1`)
- empty top-of-book levels
- trades before first book row
- trades outside previous top-of-book range
- trades outside a more tolerant previous/next top-of-book envelope
- trades outside a more tolerant previous/next visible top20 envelope

```bash
python3 -m mm_audit.orderbook_trades \
  --day-dir data/binance/BTCUSDT/20260221 \
  --exchange binance \
  --price-tol 0.0
```

Interpretation:

- `crossed_books` / `empty_top_books` are strong signals of data problems.
- `trades_outside_prev_top` is heuristic and can happen due to timing/sampling differences.
- `trades_outside_prev_next_envelope` is a stricter anomaly signal (fewer false positives than previous-top only).
- `trades_outside_prev_next_top20_envelope` is stricter still and usually the most useful “likely anomaly” counter among the trade-range metrics.

## Limits of Historical Orderbook Validation (Binance Spot)

Binance Spot public REST does not provide historical full L2 snapshots/diffs for arbitrary past timestamps.

That means:

- we **can** validate internal orderbook integrity and replay correctness
- we **cannot** fully compare recorded spot orderbook depth against Binance official historical L2 data (because it is not exposed via standard public REST)

For external orderbook validation, you would need a third-party historical L2 provider.

## Common Failure Modes

- `gzip ... unexpected end of file` / end-of-stream marker missing:
  - file was still being written, or
  - recorder stopped uncleanly (e.g. SIGTERM without clean gzip close), or
  - disk full (`No space left on device`)
- empty resync snapshot file:
  - often indicates failure during resync snapshot write path (e.g. disk full)

Use `gaps_*.csv.gz` and `events_*.csv.gz` to diagnose root cause (`fatal`, `resync_start`, `window_end`).
