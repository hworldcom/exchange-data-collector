from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional

from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import OrderBookSyncEngine, SyncResult
from mm_core.checksum.kraken import KrakenSyncEngine
from mm_recorder.exchanges.types import BookSnapshot, DepthDiff

log = logging.getLogger("mm_recorder.replay_validator")


@dataclass
class Segment:
    tag: str
    event_id: int
    recv_seq: int
    snapshot_path: Path
    checksum: Optional[int] = None
    end_recv_seq: Optional[int] = None


def _read_events(events_path: Path) -> list[dict[str, Any]]:
    rows = []
    with gzip.open(events_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return rows
        for row in reader:
            if not row:
                continue
            details = {}
            try:
                details = json.loads(row[6]) if len(row) > 6 and row[6] else {}
            except Exception:
                details = {}
            rows.append(
                {
                    "event_id": int(row[0]),
                    "recv_seq": int(row[2]),
                    "type": row[4],
                    "details": details,
                }
            )
    return rows


def _load_snapshot_csv(path: Path) -> tuple[list[list[str]], list[list[str]], Optional[int]]:
    bids: list[list[str]] = []
    asks: list[list[str]] = []
    checksum: Optional[int] = None
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        checksum_idx = None
        if header and "checksum" in header:
            checksum_idx = header.index("checksum")
        for row in reader:
            if not row:
                continue
            side = row[2]
            price = row[3]
            qty = row[4]
            if side == "bid":
                bids.append([price, qty])
            elif side == "ask":
                asks.append([price, qty])
            if checksum_idx is not None and row[checksum_idx]:
                try:
                    checksum = int(row[checksum_idx])
                except Exception:
                    pass
    return bids, asks, checksum


def _iter_diffs(diff_path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(diff_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _resolve_snapshot_path(day_dir: Path, event_id: int, tag: str, details: dict[str, Any]) -> Path:
    if details.get("path"):
        return day_dir / str(details["path"])
    return day_dir / "snapshots" / f"snapshot_{event_id:06d}_{tag}.csv"


def _build_segments(day_dir: Path, events: list[dict[str, Any]]) -> list[Segment]:
    segments: list[Segment] = []
    resync_starts: list[int] = []

    for ev in events:
        if ev["type"] == "resync_start":
            resync_starts.append(ev["recv_seq"])

    for ev in events:
        if ev["type"] != "snapshot_loaded":
            continue
        tag = str(ev["details"].get("tag", "snapshot"))
        path = _resolve_snapshot_path(day_dir, ev["event_id"], tag, ev["details"])
        checksum = ev["details"].get("checksum")
        segment = Segment(tag=tag, event_id=ev["event_id"], recv_seq=ev["recv_seq"], snapshot_path=path, checksum=checksum)
        segments.append(segment)

    # Assign end recv_seq for each segment (next resync_start or None)
    resync_starts = sorted(resync_starts)
    for seg in segments:
        next_starts = [s for s in resync_starts if s > seg.recv_seq]
        if next_starts:
            seg.end_recv_seq = next_starts[0]
    return segments


def _infer_exchange(day_dir: Path) -> str:
    parts = day_dir.parts
    if "data" in parts:
        idx = parts.index("data")
        if len(parts) > idx + 1:
            return parts[idx + 1]
    return "binance"


def _validate_segment_binance(seg: Segment, diff_path: Path) -> tuple[int, int]:
    bids, asks, _checksum = _load_snapshot_csv(seg.snapshot_path)
    lob = LocalOrderBook()
    last_update_id = 0
    with seg.snapshot_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        idx_last = header.index("lastUpdateId") if header and "lastUpdateId" in header else None
        for row in reader:
            if idx_last is not None and row[idx_last]:
                try:
                    last_update_id = int(row[idx_last])
                except Exception:
                    log.warning("Invalid lastUpdateId %r in %s", row[idx_last], seg.snapshot_path)
                break
    lob.load_snapshot(bids=bids, asks=asks, last_update_id=last_update_id)
    engine = OrderBookSyncEngine(lob)
    engine.snapshot_loaded = True

    applied = 0
    gaps = 0
    for ev in _iter_diffs(diff_path):
        recv_seq = int(ev.get("recv_seq", 0))
        if recv_seq <= seg.recv_seq:
            continue
        if seg.end_recv_seq is not None and recv_seq >= seg.end_recv_seq:
            break
        result: SyncResult = engine.feed_depth_event(ev)
        if result.action == "gap":
            gaps += 1
        else:
            applied += 1
    return applied, gaps


def _validate_segment_kraken(seg: Segment, diff_path: Path, depth_override: Optional[int]) -> tuple[int, int]:
    bids, asks, checksum = _load_snapshot_csv(seg.snapshot_path)
    snapshot = BookSnapshot(event_time_ms=0, bids=bids, asks=asks, checksum=checksum)
    depth = depth_override or max(len(bids), len(asks), 1)
    engine = KrakenSyncEngine(depth=depth)
    engine.adopt_snapshot(snapshot)

    applied = 0
    gaps = 0
    for ev in _iter_diffs(diff_path):
        recv_seq = int(ev.get("recv_seq", 0))
        if recv_seq <= seg.recv_seq:
            continue
        if seg.end_recv_seq is not None and recv_seq >= seg.end_recv_seq:
            break
        diff = DepthDiff(
            event_time_ms=int(ev.get("E", 0)),
            U=int(ev.get("U", 0)),
            u=int(ev.get("u", 0)),
            bids=ev.get("b", []),
            asks=ev.get("a", []),
            checksum=ev.get("checksum"),
            raw=ev.get("raw"),
        )
        result = engine.feed_depth_event(diff)
        if result.action == "gap":
            gaps += 1
        else:
            applied += 1
    return applied, gaps


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay validator for recorded market data")
    parser.add_argument("--day-dir", required=True, help="Path to data/<exchange>/<symbol>/<YYYYMMDD>")
    parser.add_argument("--exchange", default=None, help="Override exchange (binance|kraken)")
    args = parser.parse_args()

    day_dir = Path(args.day_dir)
    schema_path = day_dir / "schema.json"
    if not schema_path.exists():
        raise SystemExit(f"schema.json not found in {day_dir}")

    events_path = None
    kraken_depth = None
    with schema_path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    for key, info in schema.get("files", {}).items():
        if key == "events_csv":
            events_path = day_dir / info["path"]
            break
    dd = schema.get("files", {}).get("depth_diffs_ndjson_gz", {})
    if "depth" in dd:
        try:
            kraken_depth = int(dd.get("depth"))
        except Exception:
            kraken_depth = None
    if events_path is None or not events_path.exists():
        raise SystemExit("events csv not found; cannot validate")

    diff_path = day_dir / "diffs"
    diff_files = list(diff_path.glob("depth_diffs_*.ndjson.gz"))
    if not diff_files:
        raise SystemExit("no depth diffs found")
    diff_path = diff_files[0]

    events = _read_events(events_path)
    segments = _build_segments(day_dir, events)
    if not segments:
        raise SystemExit("no snapshot_loaded events found")

    exchange = (args.exchange or _infer_exchange(day_dir)).lower()
    total_applied = 0
    total_gaps = 0

    for seg in segments:
        if exchange == "kraken":
            applied, gaps = _validate_segment_kraken(seg, diff_path, kraken_depth)
        else:
            applied, gaps = _validate_segment_binance(seg, diff_path)
        total_applied += applied
        total_gaps += gaps

    print(f"segments={len(segments)} applied={total_applied} gaps={total_gaps}")
    return 1 if total_gaps else 0


if __name__ == "__main__":
    raise SystemExit(main())
