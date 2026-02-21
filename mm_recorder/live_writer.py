from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional


class LiveNdjsonWriter:
    """Append-only NDJSON writer with simple rotation + retention cleanup."""

    def __init__(
        self,
        path: Path,
        rotate_interval_s: float,
        retention_s: float,
    ) -> None:
        self.path = path
        self.rotate_interval_s = rotate_interval_s
        self.retention_s = retention_s
        self._fh: Optional[object] = None
        self._rotate_id = 0
        self._last_rotate = time.time()
        self._open()

    def _open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8", buffering=1)

    def _close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def _rotate(self) -> None:
        self._close()
        if self.path.exists() and self.path.stat().st_size > 0:
            rotated = self.path.with_name(f"{self.path.name}.{self._rotate_id}")
            self._rotate_id += 1
            os.replace(self.path, rotated)
        self._open()
        self._last_rotate = time.time()
        self._cleanup()

    def _cleanup(self) -> None:
        if self.retention_s <= 0:
            return
        now = time.time()
        base = f"{self.path.name}."
        for candidate in self.path.parent.iterdir():
            if not candidate.is_file():
                continue
            if candidate.name.startswith(base):
                age = now - candidate.stat().st_mtime
                if age > self.retention_s:
                    candidate.unlink(missing_ok=True)

    def write_line(self, line: str) -> None:
        if self._fh is None:
            self._open()
        if (time.time() - self._last_rotate) >= self.rotate_interval_s:
            self._rotate()
        assert self._fh is not None
        self._fh.write(line)

    def close(self) -> None:
        self._close()
