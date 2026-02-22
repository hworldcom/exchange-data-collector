from __future__ import annotations

import csv
import gzip
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from mm_replay.reader import ReplayDataError, iter_trades_csv, load_schema, resolve_paths


@dataclass(frozen=True)
class TopBookRow:
    recv_seq: int
    recv_time_ms: int
    epoch_id: int
    bid1_price: float
    bid1_qty: float
    ask1_price: float
    ask1_qty: float
    visible_bid_min: float | None = None
    visible_ask_max: float | None = None


@dataclass
class OrderbookTradeConsistencyReport:
    day_dir: str
    exchange: str
    symbol: str
    books_total: int = 0
    trades_total: int = 0
    crossed_books: int = 0
    empty_top_books: int = 0
    trades_without_prev_book: int = 0
    trades_outside_prev_top: int = 0
    trades_outside_prev_next_envelope: int = 0
    trades_outside_prev_next_top20_envelope: int = 0
    samples: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "day_dir": self.day_dir,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "books_total": self.books_total,
            "trades_total": self.trades_total,
            "crossed_books": self.crossed_books,
            "empty_top_books": self.empty_top_books,
            "trades_without_prev_book": self.trades_without_prev_book,
            "trades_outside_prev_top": self.trades_outside_prev_top,
            "trades_outside_prev_next_envelope": self.trades_outside_prev_next_envelope,
            "trades_outside_prev_next_top20_envelope": self.trades_outside_prev_next_top20_envelope,
            "samples": self.samples,
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


def iter_topbook_rows(path: Path) -> Iterator[TopBookRow]:
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            required = [
                "recv_seq",
                "recv_time_ms",
                "epoch_id",
                "bid1_price",
                "bid1_qty",
                "ask1_price",
                "ask1_qty",
            ]
            fields = reader.fieldnames or []
            missing = [k for k in required if k not in fields]
            if missing:
                raise ReplayDataError(f"Missing topbook columns in {path}: {missing}")
            for lineno, row in enumerate(reader, start=2):
                if not row:
                    continue
                try:
                    visible_bids: list[float] = []
                    visible_asks: list[float] = []
                    # Best-effort parse of flattened top-N columns (usually 20 levels).
                    for i in range(1, 21):
                        bp_key = f"bid{i}_price"
                        bq_key = f"bid{i}_qty"
                        ap_key = f"ask{i}_price"
                        aq_key = f"ask{i}_qty"
                        if bp_key in row and bq_key in row:
                            bp = float(row[bp_key])
                            bq = float(row[bq_key])
                            if bp > 0 and bq > 0:
                                visible_bids.append(bp)
                        if ap_key in row and aq_key in row:
                            ap = float(row[ap_key])
                            aq = float(row[aq_key])
                            if ap > 0 and aq > 0:
                                visible_asks.append(ap)
                    yield TopBookRow(
                        recv_seq=int(row["recv_seq"]),
                        recv_time_ms=int(row["recv_time_ms"]),
                        epoch_id=int(row.get("epoch_id") or 0),
                        bid1_price=float(row["bid1_price"]),
                        bid1_qty=float(row["bid1_qty"]),
                        ask1_price=float(row["ask1_price"]),
                        ask1_qty=float(row["ask1_qty"]),
                        visible_bid_min=(min(visible_bids) if visible_bids else None),
                        visible_ask_max=(max(visible_asks) if visible_asks else None),
                    )
                except Exception as exc:
                    raise ReplayDataError(f"Malformed orderbook row at {path}:{lineno}") from exc
    except ReplayDataError:
        raise
    except Exception as exc:
        raise ReplayDataError(f"Failed reading orderbook csv {path}: {exc}") from exc


def audit_orderbook_trade_consistency(
    *,
    day_dir: Path,
    exchange: str = "binance",
    symbol: str | None = None,
    price_tol: float = 0.0,
    max_samples: int = 20,
) -> OrderbookTradeConsistencyReport:
    day_dir = Path(day_dir)
    paths = resolve_paths(day_dir, exchange=exchange)
    if paths.trades_csv_path is None:
        raise ReplayDataError(f"No trades_ws csv found under {day_dir}")
    ob_path = _resolve_orderbook_csv_path(day_dir)
    symbol_out = symbol or day_dir.parent.name
    report = OrderbookTradeConsistencyReport(day_dir=str(day_dir), exchange=exchange, symbol=symbol_out)

    book_iter = iter(iter_topbook_rows(ob_path))
    prev_book: TopBookRow | None = None
    next_book: TopBookRow | None = next(book_iter, None)

    # Pre-scan books for basic integrity and keep stream position.
    # Since we need the iterator later, store rows in memory once (top1 only).
    books = []
    if next_book is not None:
        books.append(next_book)
        for b in book_iter:
            books.append(b)
    report.books_total = len(books)
    for b in books:
        if b.bid1_price <= 0 or b.ask1_price <= 0:
            report.empty_top_books += 1
            if len(report.samples) < max_samples:
                report.samples.append({"kind": "empty_topbook", "recv_seq": b.recv_seq})
            continue
        if b.bid1_price - b.ask1_price > price_tol:
            report.crossed_books += 1
            if len(report.samples) < max_samples:
                report.samples.append(
                    {
                        "kind": "crossed_topbook",
                        "recv_seq": b.recv_seq,
                        "bid1_price": b.bid1_price,
                        "ask1_price": b.ask1_price,
                    }
                )

    # Merge trades against topbook rows by recv_seq.
    i = 0
    n = len(books)
    for tr in iter_trades_csv(paths.trades_csv_path):
        report.trades_total += 1
        while i < n and books[i].recv_seq <= int(tr.recv_seq):
            prev_book = books[i]
            i += 1
        next_book = books[i] if i < n else None

        if prev_book is None:
            report.trades_without_prev_book += 1
            if len(report.samples) < max_samples:
                report.samples.append({"kind": "no_prev_book", "trade_recv_seq": int(tr.recv_seq), "trade_id": int(tr.trade_id)})
            continue

        px = float(tr.price)
        prev_ok = (prev_book.bid1_price - price_tol) <= px <= (prev_book.ask1_price + price_tol)
        if not prev_ok:
            report.trades_outside_prev_top += 1
            if len(report.samples) < max_samples:
                report.samples.append(
                    {
                        "kind": "outside_prev_top",
                        "trade_recv_seq": int(tr.recv_seq),
                        "trade_id": int(tr.trade_id),
                        "trade_price": px,
                        "prev_recv_seq": prev_book.recv_seq,
                        "prev_bid1": prev_book.bid1_price,
                        "prev_ask1": prev_book.ask1_price,
                    }
                )

        # More tolerant check using envelope of previous and next topbook snapshots in the same epoch.
        env_ok = prev_ok
        if not env_ok and next_book is not None and next_book.epoch_id == prev_book.epoch_id:
            lo = min(prev_book.bid1_price, next_book.bid1_price) - price_tol
            hi = max(prev_book.ask1_price, next_book.ask1_price) + price_tol
            env_ok = lo <= px <= hi
        if not env_ok:
            report.trades_outside_prev_next_envelope += 1
            if len(report.samples) < max_samples:
                report.samples.append(
                    {
                        "kind": "outside_prev_next_envelope",
                        "trade_recv_seq": int(tr.recv_seq),
                        "trade_id": int(tr.trade_id),
                        "trade_price": px,
                        "prev_recv_seq": prev_book.recv_seq,
                        "next_recv_seq": (next_book.recv_seq if next_book is not None else None),
                    }
                )

        # Even more tolerant check using visible top-N envelope (top20 in recorder output).
        env20_ok = prev_ok
        if not env20_ok and next_book is not None and next_book.epoch_id == prev_book.epoch_id:
            mins = [v for v in (prev_book.visible_bid_min, next_book.visible_bid_min) if v is not None]
            maxs = [v for v in (prev_book.visible_ask_max, next_book.visible_ask_max) if v is not None]
            if mins and maxs:
                lo20 = min(mins) - price_tol
                hi20 = max(maxs) + price_tol
                env20_ok = lo20 <= px <= hi20
        if not env20_ok:
            report.trades_outside_prev_next_top20_envelope += 1
            if len(report.samples) < max_samples:
                report.samples.append(
                    {
                        "kind": "outside_prev_next_top20_envelope",
                        "trade_recv_seq": int(tr.recv_seq),
                        "trade_id": int(tr.trade_id),
                        "trade_price": px,
                        "prev_recv_seq": prev_book.recv_seq,
                        "next_recv_seq": (next_book.recv_seq if next_book is not None else None),
                        "prev_visible_bid_min": prev_book.visible_bid_min,
                        "prev_visible_ask_max": prev_book.visible_ask_max,
                        "next_visible_bid_min": (next_book.visible_bid_min if next_book is not None else None),
                        "next_visible_ask_max": (next_book.visible_ask_max if next_book is not None else None),
                    }
                )

    return report


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Audit consistency between recorded orderbook top-of-book and recorded trades")
    parser.add_argument("--day-dir", required=True, help="Path to data/<exchange>/<symbol>/<YYYYMMDD>")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--price-tol", type=float, default=0.0, help="Absolute tolerance for price range checks")
    parser.add_argument("--max-samples", type=int, default=20)
    args = parser.parse_args(argv)

    try:
        report = audit_orderbook_trade_consistency(
            day_dir=Path(args.day_dir),
            exchange=str(args.exchange),
            symbol=args.symbol,
            price_tol=float(args.price_tol),
            max_samples=max(0, int(args.max_samples)),
        )
    except ReplayDataError as exc:
        print(f"audit_error: {exc}", file=sys.stderr)
        return 2

    print(json.dumps({"type": "orderbook_trade_consistency", **report.as_dict()}))
    # This is an audit heuristic, so only hard fail on clearly invalid book states.
    return 0 if report.crossed_books == 0 and report.empty_top_books == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
