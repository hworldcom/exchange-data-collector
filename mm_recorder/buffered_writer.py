from __future__ import annotations

import csv
import gzip
import time
from pathlib import Path
from typing import Iterable, Sequence


def _is_empty_text_file(path: Path) -> bool:
    if not path.exists():
        return True
    if path.suffix == ".gz":
        try:
            with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
                return f.readline() == ""
        except Exception:
            return False
    return path.stat().st_size == 0


class BufferedCSVWriter:
    """Batch rows in memory before flushing to disk to reduce fsync pressure."""

    def __init__(
        self,
        path: str | Path,
        header: Sequence[str] | None = None,
        flush_rows: int = 1000,
        flush_interval_s: float = 1.0,
        opener=None,
    ) -> None:
        self.path = Path(path)
        self.header = list(header) if header else None
        self.flush_rows = max(1, flush_rows)
        self.flush_interval_s = max(0.0, float(flush_interval_s))
        self.opener = opener  # optional callable(path) -> file-like

        self._buffer: list[list[str]] = []
        self._file = None
        self._writer: csv.writer | None = None
        self._last_flush = time.monotonic()

    def _ensure_open(self) -> None:
        if self._file is not None:
            return

        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        f = None
        writer = None
        try:
            if self.opener is not None:
                f = self.opener(self.path)
            elif self.path.suffix == '.gz':
                f = gzip.open(self.path, 'at', encoding='utf-8', newline='')
            else:
                f = self.path.open('a', newline='')
            writer = csv.writer(f)

            is_empty = (not existed) or _is_empty_text_file(self.path)
            if self.header and is_empty:
                writer.writerow(self.header)
                f.flush()
        except Exception:
            if f is not None:
                try:
                    f.close()
                except Exception:
                    pass
            raise
        else:
            self._file = f
            self._writer = writer

    def write_row(self, row: Sequence[str | int | float]) -> None:
        self._ensure_open()
        self._buffer.append([str(v) for v in row])
        if self._should_flush():
            self.flush()

    def write_rows(self, rows: Iterable[Sequence[str | int | float]]) -> None:
        for row in rows:
            self.write_row(row)

    def flush(self) -> None:
        if not self._buffer or self._writer is None or self._file is None:
            self._last_flush = time.monotonic()
            return

        self._writer.writerows(self._buffer)
        self._file.flush()
        self._buffer.clear()
        self._last_flush = time.monotonic()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._file is not None:
                self._file.close()
                self._file = None
                self._writer = None

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self.flush_rows:
            return True
        if self.flush_interval_s == 0.0:
            return False
        return (time.monotonic() - self._last_flush) >= self.flush_interval_s

    def __enter__(self) -> "BufferedCSVWriter":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def ensure_file(self) -> None:
        """Ensure the backing file exists with headers even if no rows are written yet."""
        self._ensure_open()


class BufferedTextWriter:
    """Batch text lines in memory before flushing to disk.

    This is intended for high-frequency, append-only logs where flushing every line would
    induce unnecessary I/O pressure and distort timestamps.
    """

    def __init__(
        self,
        path: str | Path,
        flush_lines: int = 5000,
        flush_interval_s: float = 1.0,
        opener=None,
    ) -> None:
        self.path = Path(path)
        self.flush_lines = max(1, int(flush_lines))
        self.flush_interval_s = max(0.0, float(flush_interval_s))
        self.opener = opener  # optional callable(path) -> file-like

        self._buffer: list[str] = []
        self._file = None
        self._last_flush = time.monotonic()

    def _ensure_open(self) -> None:
        if self._file is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        f = None
        try:
            if self.opener is not None:
                f = self.opener(self.path)
            else:
                # Default to plain text append
                f = self.path.open("a", encoding="utf-8")
        except Exception:
            if f is not None:
                try:
                    f.close()
                except Exception:
                    pass
            raise
        else:
            self._file = f

    def write_line(self, line: str) -> None:
        self._ensure_open()
        self._buffer.append(line)
        if self._should_flush():
            self.flush()

    def flush(self) -> None:
        if not self._buffer or self._file is None:
            self._last_flush = time.monotonic()
            return
        self._file.writelines(self._buffer)
        self._file.flush()
        self._buffer.clear()
        self._last_flush = time.monotonic()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._file is not None:
                self._file.close()
                self._file = None

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self.flush_lines:
            return True
        if self.flush_interval_s == 0.0:
            return False
        return (time.monotonic() - self._last_flush) >= self.flush_interval_s

    def __enter__(self) -> "BufferedTextWriter":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
