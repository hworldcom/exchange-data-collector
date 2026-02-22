from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional


class ReplayDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplayPaths:
    schema_path: Path
    events_path: Path
    diff_path: Path
    exchange: str
    depth: Optional[int] = None


@dataclass(frozen=True)
class ReplayEvent:
    event_id: int
    recv_time_ms: int
    recv_seq: int
    run_id: int
    typ: str
    epoch_id: int
    details: dict[str, Any]


@dataclass(frozen=True)
class ReplaySegment:
    index: int
    tag: str
    event_id: int
    recv_seq: int
    epoch_id: int
    snapshot_path: Path
    checksum: Optional[int] = None
    end_recv_seq: Optional[int] = None


def load_schema(day_dir: Path) -> dict[str, Any]:
    schema_path = day_dir / "schema.json"
    if not schema_path.exists():
        raise ReplayDataError(f"schema.json not found in {day_dir}")
    with schema_path.open("r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as exc:
            raise ReplayDataError(f"Invalid schema.json in {day_dir}: {exc}") from exc


def infer_exchange(day_dir: Path) -> str:
    parts = day_dir.parts
    if "data" in parts:
        idx = parts.index("data")
        if len(parts) > idx + 1:
            return str(parts[idx + 1]).lower()
    return "binance"


def resolve_paths(day_dir: Path, exchange: str | None = None) -> ReplayPaths:
    schema = load_schema(day_dir)
    schema_version = int(schema.get("schema_version", 0))
    if schema_version != 4:
        raise ReplayDataError(f"Unsupported schema_version={schema_version}; expected 4")
    files = schema.get("files") or {}
    events_info = files.get("events_csv")
    if not isinstance(events_info, dict) or not events_info.get("path"):
        raise ReplayDataError("schema.json missing files.events_csv.path")
    events_path = day_dir / str(events_info["path"])
    if not events_path.exists():
        raise ReplayDataError(f"events csv not found: {events_path}")

    diff_info = files.get("depth_diffs_ndjson_gz")
    diff_path: Optional[Path] = None
    depth: Optional[int] = None
    if isinstance(diff_info, dict) and diff_info.get("path"):
        diff_path = day_dir / str(diff_info["path"])
        try:
            if "depth" in diff_info:
                depth = int(diff_info["depth"])
        except Exception:
            depth = None
    if diff_path is None or not diff_path.exists():
        # Fallback for older/manual datasets
        candidates = sorted((day_dir / "diffs").glob("depth_diffs_*.ndjson*"))
        if not candidates:
            raise ReplayDataError(f"No depth diffs found under {day_dir}")
        diff_path = candidates[0]

    return ReplayPaths(
        schema_path=day_dir / "schema.json",
        events_path=events_path,
        diff_path=diff_path,
        exchange=(exchange or infer_exchange(day_dir)).lower(),
        depth=depth,
    )


def read_events(events_path: Path) -> list[ReplayEvent]:
    rows: list[ReplayEvent] = []
    try:
        with gzip.open(events_path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return rows
            for row in reader:
                if not row:
                    continue
                if len(row) < 6:
                    raise ReplayDataError(f"Malformed event row in {events_path}: {row!r}")
                details: dict[str, Any] = {}
                if len(row) > 6 and row[6]:
                    try:
                        parsed = json.loads(row[6])
                        details = parsed if isinstance(parsed, dict) else {}
                    except json.JSONDecodeError as exc:
                        raise ReplayDataError(
                            f"Invalid details_json in {events_path} row event_id={row[0]!r}: {exc}"
                        ) from exc
                rows.append(
                    ReplayEvent(
                        event_id=int(row[0]),
                        recv_time_ms=int(row[1]),
                        recv_seq=int(row[2]),
                        run_id=int(row[3]),
                        typ=str(row[4]),
                        epoch_id=int(row[5]),
                        details=details,
                    )
                )
    except Exception as exc:
        raise ReplayDataError(f"Failed reading events {events_path}: {exc}") from exc
    return rows


def _resolve_snapshot_path(day_dir: Path, event_id: int, tag: str, details: dict[str, Any]) -> Path:
    if details.get("path"):
        return day_dir / str(details["path"])
    return day_dir / "snapshots" / f"snapshot_{event_id:06d}_{tag}.csv"


def build_segments(day_dir: Path, events: list[ReplayEvent]) -> list[ReplaySegment]:
    resync_starts = sorted(ev.recv_seq for ev in events if ev.typ == "resync_start")
    segments: list[ReplaySegment] = []
    for ev in events:
        if ev.typ != "snapshot_loaded":
            continue
        tag = str(ev.details.get("tag", "snapshot"))
        path = _resolve_snapshot_path(day_dir, ev.event_id, tag, ev.details)
        checksum = ev.details.get("checksum")
        seg = ReplaySegment(
            index=len(segments),
            tag=tag,
            event_id=ev.event_id,
            recv_seq=ev.recv_seq,
            epoch_id=ev.epoch_id,
            snapshot_path=path,
            checksum=(int(checksum) if checksum is not None else None),
            end_recv_seq=None,
        )
        segments.append(seg)
    out: list[ReplaySegment] = []
    for seg in segments:
        next_boundaries = [s for s in resync_starts if s > seg.recv_seq]
        end_recv_seq = next_boundaries[0] if next_boundaries else None
        out.append(
            ReplaySegment(
                index=seg.index,
                tag=seg.tag,
                event_id=seg.event_id,
                recv_seq=seg.recv_seq,
                epoch_id=seg.epoch_id,
                snapshot_path=seg.snapshot_path,
                checksum=seg.checksum,
                end_recv_seq=end_recv_seq,
            )
        )
    return out


@dataclass(frozen=True)
class SnapshotCsv:
    bids: list[list[str]]
    asks: list[list[str]]
    last_update_id: int
    checksum: Optional[int] = None


def load_snapshot_csv(path: Path) -> SnapshotCsv:
    if not path.exists():
        raise ReplayDataError(f"Snapshot file not found: {path}")
    bids: list[list[str]] = []
    asks: list[list[str]] = []
    checksum: Optional[int] = None
    last_update_id = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                raise ReplayDataError(f"Empty snapshot csv: {path}")
            checksum_idx = header.index("checksum") if "checksum" in header else None
            last_idx = header.index("lastUpdateId") if "lastUpdateId" in header else None
            for row in reader:
                if not row:
                    continue
                if len(row) < 5:
                    raise ReplayDataError(f"Malformed snapshot row in {path}: {row!r}")
                side = row[2]
                price = row[3]
                qty = row[4]
                if side == "bid":
                    bids.append([price, qty])
                elif side == "ask":
                    asks.append([price, qty])
                if checksum_idx is not None and checksum is None and len(row) > checksum_idx and row[checksum_idx]:
                    checksum = int(row[checksum_idx])
                if last_idx is not None and len(row) > last_idx and row[last_idx]:
                    try:
                        last_update_id = int(row[last_idx])
                    except Exception:
                        pass
    except ReplayDataError:
        raise
    except Exception as exc:
        raise ReplayDataError(f"Failed reading snapshot {path}: {exc}") from exc
    return SnapshotCsv(bids=bids, asks=asks, last_update_id=last_update_id, checksum=checksum)


def iter_diffs(diff_path: Path) -> Iterator[dict[str, Any]]:
    opener = gzip.open if diff_path.suffix == ".gz" else open
    try:
        with opener(diff_path, "rt", encoding="utf-8") as f:  # type: ignore[arg-type]
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ReplayDataError(f"Invalid diff JSON at {diff_path}:{lineno}: {exc}") from exc
                if not isinstance(payload, dict):
                    raise ReplayDataError(f"Diff payload must be object at {diff_path}:{lineno}")
                yield payload
    except ReplayDataError:
        raise
    except Exception as exc:
        raise ReplayDataError(f"Failed reading diffs {diff_path}: {exc}") from exc
