from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


CHECKPOINT_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class FileFingerprint:
    path: str
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class RecvSeqCheckpoint:
    schema_version: int
    last_recv_seq: int
    updated_utc: str
    files: dict[str, FileFingerprint]


def load(path: Path) -> RecvSeqCheckpoint | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("checkpoint payload must be a JSON object")
    files_payload = payload.get("files")
    if not isinstance(files_payload, dict):
        raise ValueError("checkpoint files must be a JSON object")
    files: dict[str, FileFingerprint] = {}
    for logical_name, entry in files_payload.items():
        if not isinstance(logical_name, str) or not isinstance(entry, dict):
            raise ValueError("checkpoint file entries must be objects keyed by logical name")
        files[logical_name] = FileFingerprint(
            path=str(entry["path"]),
            size=int(entry["size"]),
            mtime_ns=int(entry["mtime_ns"]),
        )
    return RecvSeqCheckpoint(
        schema_version=int(payload["schema_version"]),
        last_recv_seq=int(payload["last_recv_seq"]),
        updated_utc=str(payload["updated_utc"]),
        files=files,
    )


def write_atomic(path: Path, checkpoint: RecvSeqCheckpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    payload = {
        "schema_version": int(checkpoint.schema_version),
        "last_recv_seq": int(checkpoint.last_recv_seq),
        "updated_utc": checkpoint.updated_utc,
        "files": {
            logical_name: {
                "path": fp.path,
                "size": int(fp.size),
                "mtime_ns": int(fp.mtime_ns),
            }
            for logical_name, fp in checkpoint.files.items()
        },
    }
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, sort_keys=True)
        f.flush()
    os.replace(tmp_path, path)


def matches_current_files(checkpoint: RecvSeqCheckpoint, current_files: dict[str, FileFingerprint]) -> bool:
    if int(checkpoint.schema_version) != CHECKPOINT_SCHEMA_VERSION:
        return False
    if set(checkpoint.files.keys()) != set(current_files.keys()):
        return False
    for logical_name, current in current_files.items():
        recorded = checkpoint.files.get(logical_name)
        if recorded is None:
            return False
        if recorded.path != current.path:
            return False
        if int(recorded.size) != int(current.size):
            return False
        if int(recorded.mtime_ns) != int(current.mtime_ns):
            return False
    return True


def advance(
    checkpoint: RecvSeqCheckpoint | None,
    last_recv_seq: int,
    current_files: dict[str, FileFingerprint],
) -> RecvSeqCheckpoint:
    previous = int(checkpoint.last_recv_seq) if checkpoint is not None else 0
    return RecvSeqCheckpoint(
        schema_version=CHECKPOINT_SCHEMA_VERSION,
        last_recv_seq=max(previous, int(last_recv_seq)),
        updated_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        files=dict(current_files),
    )
