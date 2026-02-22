from __future__ import annotations

import csv
import gzip
import json
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from mm_replay.orderbook import ReplayConfig, ReplayIntegrityError, ReplayStats, replay_orderbook_day
from mm_replay.reader import ReplayDataError, load_schema, read_events, resolve_paths


@dataclass
class ParityReport:
    day_dir: str
    exchange: str
    top_n: int
    compared_rows: int = 0
    replay_book_frames: int = 0
    recorded_rows: int = 0
    mismatches: int = 0
    recv_seq_mismatches: int = 0
    extra_replay_frames: int = 0
    missing_recorded_rows: int = 0
    samples: list[dict[str, Any]] = field(default_factory=list)
    replay_stats: Optional[ReplayStats] = None

    def ok(self) -> bool:
        return (
            self.mismatches == 0
            and self.recv_seq_mismatches == 0
            and self.extra_replay_frames == 0
            and self.missing_recorded_rows == 0
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "day_dir": self.day_dir,
            "exchange": self.exchange,
            "top_n": self.top_n,
            "compared_rows": self.compared_rows,
            "replay_book_frames": self.replay_book_frames,
            "recorded_rows": self.recorded_rows,
            "mismatches": self.mismatches,
            "recv_seq_mismatches": self.recv_seq_mismatches,
            "extra_replay_frames": self.extra_replay_frames,
            "missing_recorded_rows": self.missing_recorded_rows,
            "ok": self.ok(),
            "samples": self.samples,
            "replay_stats": self.replay_stats.as_dict() if self.replay_stats is not None else None,
        }


def _resolve_orderbook_csv_path(day_dir: Path) -> Path:
    schema = load_schema(day_dir)
    files = schema.get("files") or {}
    ob_info = files.get("orderbook_ws_depth_csv")
    if isinstance(ob_info, dict) and ob_info.get("path"):
        p = day_dir / str(ob_info["path"])
        if p.exists():
            return p
    candidates = sorted(day_dir.glob("orderbook_ws_depth_*.csv.gz"))
    if not candidates:
        raise ReplayDataError(f"orderbook_ws_depth csv not found under {day_dir}")
    return candidates[0]


def _first_window_end_recv_seq(day_dir: Path, exchange: str | None) -> Optional[int]:
    try:
        paths = resolve_paths(day_dir, exchange=exchange)
        events = read_events(paths.events_path)
    except Exception:
        return None
    vals = [int(ev.recv_seq) for ev in events if ev.typ == "window_end"]
    return min(vals) if vals else None


def _iter_recorded_orderbook_rows(path: Path, top_n: int) -> Iterator[dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return
            required = ["recv_seq", "epoch_id"]
            for i in range(1, top_n + 1):
                required.extend([f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"])
            missing = [k for k in required if k not in (reader.fieldnames or [])]
            if missing:
                raise ReplayDataError(f"Missing orderbook columns in {path}: {missing}")
            for lineno, row in enumerate(reader, start=2):
                if not row:
                    continue
                try:
                    bids = [[float(row[f"bid{i}_price"]), float(row[f"bid{i}_qty"])] for i in range(1, top_n + 1)]
                    asks = [[float(row[f"ask{i}_price"]), float(row[f"ask{i}_qty"])] for i in range(1, top_n + 1)]
                    yield {
                        "recv_seq": int(row["recv_seq"]),
                        "epoch_id": int(row.get("epoch_id") or 0),
                        "bids": bids,
                        "asks": asks,
                    }
                except Exception as exc:
                    raise ReplayDataError(f"Malformed orderbook row at {path}:{lineno}") from exc
    except ReplayDataError:
        raise
    except Exception as exc:
        raise ReplayDataError(f"Failed reading orderbook csv {path}: {exc}") from exc


def _pad_levels(levels: list[list[float]], n: int) -> list[list[float]]:
    out = [[float(p), float(q)] for p, q in levels[:n]]
    if len(out) < n:
        out.extend([[0.0, 0.0] for _ in range(n - len(out))])
    return out


def _same_num(a: float, b: float, tol: float) -> bool:
    return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=tol)


def _compare_frame_to_row(frame: dict[str, Any], row: dict[str, Any], top_n: int, tol: float) -> list[str]:
    errors: list[str] = []
    if int(frame.get("recv_seq", 0)) != int(row["recv_seq"]):
        errors.append(f"recv_seq frame={frame.get('recv_seq')} row={row['recv_seq']}")
        return errors
    if int(frame.get("epoch_id", 0)) != int(row["epoch_id"]):
        errors.append(f"epoch_id frame={frame.get('epoch_id')} row={row['epoch_id']}")
    frame_bids = _pad_levels(frame.get("bids", []), top_n)
    frame_asks = _pad_levels(frame.get("asks", []), top_n)
    for side_name, lhs, rhs in (("bid", frame_bids, row["bids"]), ("ask", frame_asks, row["asks"])):
        for idx in range(top_n):
            fp, fq = lhs[idx]
            rp, rq = rhs[idx]
            if not _same_num(fp, rp, tol):
                errors.append(f"{side_name}{idx+1}_price frame={fp} row={rp}")
            if not _same_num(fq, rq, tol):
                errors.append(f"{side_name}{idx+1}_qty frame={fq} row={rq}")
    return errors


def compare_replay_to_recorded_orderbook(
    *,
    day_dir: Path,
    exchange: str | None = None,
    top_n: int = 20,
    tol: float = 1e-8,
    max_samples: int = 10,
) -> ParityReport:
    day_dir = Path(day_dir)
    ob_path = _resolve_orderbook_csv_path(day_dir)
    recorded_iter = iter(_iter_recorded_orderbook_rows(ob_path, top_n))
    report = ParityReport(day_dir=str(day_dir), exchange=(exchange or day_dir.parent.parent.name), top_n=top_n)
    window_end_recv_seq = _first_window_end_recv_seq(day_dir, exchange)
    next_row = next(recorded_iter, None)

    def emit(frame: dict[str, Any]) -> None:
        nonlocal next_row
        if frame.get("type") != "book":
            return
        report.replay_book_frames += 1
        if next_row is None:
            report.extra_replay_frames += 1
            if len(report.samples) < max_samples:
                report.samples.append({"kind": "extra_replay", "recv_seq": frame.get("recv_seq")})
            return
        errors = _compare_frame_to_row(frame, next_row, top_n, tol)
        if errors:
            if errors and errors[0].startswith("recv_seq "):
                report.recv_seq_mismatches += 1
            else:
                report.mismatches += 1
            if len(report.samples) < max_samples:
                report.samples.append(
                    {
                        "kind": "mismatch",
                        "frame_recv_seq": int(frame.get("recv_seq", 0)),
                        "row_recv_seq": int(next_row["recv_seq"]),
                        "errors": errors[:8],
                    }
                )
        report.compared_rows += 1
        next_row = next(recorded_iter, None)

    stats = replay_orderbook_day(
        ReplayConfig(
            day_dir=day_dir,
            exchange=exchange,
            top_n=top_n,
            on_error="strict",
            speed=0.0,
            validate_only=False,
            include_trades=False,
        ),
        emit=emit,
    )
    report.replay_stats = stats

    # Count rows consumed + remaining.
    if next_row is not None:
        report.recorded_rows += 1
        report.missing_recorded_rows += 1
    for _ in recorded_iter:
        report.recorded_rows += 1
        report.missing_recorded_rows += 1
    report.recorded_rows += report.compared_rows

    # Recorder can write the final depth diff to diffs file, then emit `window_end`,
    # and return before writing an orderbook row for that same depth message.
    # In that case replay produces exactly one extra trailing book frame at recv_seq=window_end-1.
    if (
        window_end_recv_seq is not None
        and report.extra_replay_frames == 1
        and report.mismatches == 0
        and report.recv_seq_mismatches == 0
        and report.samples
        and report.samples[-1].get("kind") == "extra_replay"
        and int(report.samples[-1].get("recv_seq", -1)) == int(window_end_recv_seq) - 1
    ):
        report.extra_replay_frames = 0
        report.samples[-1]["kind"] = "ignored_window_end_trailing_depth"

    return report


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Compare mm_replay orderbook output against recorded orderbook CSV")
    parser.add_argument("--day-dir", required=True, help="Path to data/<exchange>/<symbol>/<YYYYMMDD>")
    parser.add_argument("--exchange", default=None, help="Override exchange (binance|kraken|bitfinex)")
    parser.add_argument("--top-n", type=int, default=20, help="Number of levels to compare")
    parser.add_argument("--tol", type=float, default=1e-8, help="Absolute tolerance for numeric comparisons")
    parser.add_argument("--max-samples", type=int, default=10, help="Max mismatch samples to print")
    args = parser.parse_args(argv)

    try:
        report = compare_replay_to_recorded_orderbook(
            day_dir=Path(args.day_dir),
            exchange=args.exchange,
            top_n=max(1, int(args.top_n)),
            tol=float(args.tol),
            max_samples=max(0, int(args.max_samples)),
        )
    except (ReplayDataError, ReplayIntegrityError) as exc:
        print(f"parity_error: {exc}", file=sys.stderr)
        return 2

    print(json.dumps({"type": "parity_summary", **report.as_dict()}))
    return 0 if report.ok() else 1


if __name__ == "__main__":
    raise SystemExit(main())
