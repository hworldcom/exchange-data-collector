# Sync & Checksum Logic (Binance / Kraken / Bitfinex)

This document describes how each exchange syncs order books, how we validate integrity,
and issues we encountered while implementing the logic.

## Binance (Spot) — Sequence-based sync

### Data model
- WS depth updates: `U` (first update id), `u` (last update id), `b`/`a` arrays.
- REST snapshot: `lastUpdateId`.

### Sync algorithm
1) Buffer WS updates until snapshot is loaded.
2) Discard updates with `u <= lastUpdateId`.
3) Find the first update where `U <= lastUpdateId+1 <= u` (bridge).
4) Apply that update, then apply all sequential updates where `U == lastUpdateId+1`.
5) If a gap is detected → resync.

### Example
```
snapshot lastUpdateId = 100
WS update: U=98, u=101  -> bridge (covers 101)
WS update: U=102, u=103 -> apply
```

### Issues found
- None critical so far; the sequence engine is stable.

---

## Kraken (WS v2) — Checksum-based sync

### Data model
- WS snapshot & update: `bids`, `asks`, `checksum`, `timestamp`.
- No sequence IDs.

### Checksum logic
- Maintain top-N levels (where N is the subscribed depth).
- Compute CRC32 on the **top 10 levels** (per Kraken spec).
- If checksum mismatches → resync.

### Example
```
snapshot: bids/asks + checksum
update: delta + checksum
if computed_checksum != received_checksum => resync
```

### Issues found
- `timestamp` can be RFC3339; we had to parse ISO strings.
- `qty` can be `"0.00000000"` → must treat as delete.
- Depth must be one of `{10,25,100,500,1000}`; wrong depth yields no data.
- Must trim the book to subscribed depth or checksums mismatch.

---

## Bitfinex (WS v2) — Checksum frames

### Data model
- Snapshot: `[chanId, [[price, count, amount], ...]]`
- Update: `[chanId, price, count, amount]` **or** `[chanId, [price, count, amount]]`
- Checksum frame: `[chanId, "cs", checksum]`
- Trades: `[chanId, "tu", trade_id, mts, amount, price]`

### Checksum logic
- Use **top 25 bids** + **top 25 asks**.
- Build the checksum string **interleaved**:
```
bid0.price:bid0.amount:ask0.price:ask0.amount:
bid1.price:bid1.amount:ask1.price:ask1.amount:...
```
- Bids amount positive, asks amount negative.
- CRC32 (signed) of the string.

### Example (from Bitfinex docs)
```
Bids: {6000:1, 5900:2}
Asks: {6100:-3, 6200:-4}
Checksum string:
6000:1:6100:-3:5900:2:6200:-4
```

### Issues found
1) **Missing update shape**: Bitfinex sometimes sends `[chanId, [price, count, amount]]`.
2) **Wrong checksum payload**: initial implementation included `count`; correct payload is price + amount only.
3) **Sign handling**: asks must be negative in the checksum string.
4) **String formatting**: must preserve exchange numeric values as-is.

---

## Current Implementation Summary

| Exchange | Sync Type | Engine | Integrity Validation |
|---------|-----------|--------|----------------------|
| Binance | Sequence | `OrderBookSyncEngine` | `U/u` bridging + gap detection |
| Kraken  | Checksum | `KrakenSyncEngine` | CRC32 on top 10 |
| Bitfinex | Checksum | `BitfinexSyncEngine` | CRC32 (signed), interleaved top 25 |

