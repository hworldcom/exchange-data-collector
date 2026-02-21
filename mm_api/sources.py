from __future__ import annotations

from pathlib import Path
import re
from typing import Optional

from mm_core.symbols import symbol_fs as symbol_fs_fn

def _latest_day_dir(root: Path) -> Optional[Path]:
    if not root.exists():
        return None
    day_dirs = [p for p in root.iterdir() if p.is_dir() and p.name.isdigit()]
    if not day_dirs:
        return None
    return sorted(day_dirs, key=lambda p: p.name)[-1]


_EXCHANGE_RE = re.compile(r"^[a-z0-9_]+$")
_SYMBOL_RE = re.compile(r"^[A-Za-z0-9/:\- ]+$")


def sanitize_exchange(exchange: str) -> str:
    exchange = (exchange or "").strip().lower()
    if not exchange or ".." in exchange or not _EXCHANGE_RE.match(exchange):
        raise ValueError("invalid exchange")
    return exchange


def sanitize_symbol(symbol: str) -> str:
    symbol = (symbol or "").strip()
    if not symbol or ".." in symbol or "\\" in symbol or not _SYMBOL_RE.match(symbol):
        raise ValueError("invalid symbol")
    return symbol


def resolve_latest_paths(exchange: str, symbol: str) -> dict:
    exchange = sanitize_exchange(exchange)
    symbol = sanitize_symbol(symbol)
    symbol_fs = symbol_fs_fn(symbol, upper=True)
    if not symbol_fs or symbol_fs in {".", ".."}:
        raise ValueError("invalid symbol")
    base = Path("data") / exchange / symbol_fs
    day_dir = _latest_day_dir(base)
    if day_dir is None:
        return {}
    diffs = day_dir / "diffs"
    trades = day_dir / "trades"
    snapshots = day_dir / "snapshots"
    live = day_dir / "live"
    paths = {
        "day_dir": day_dir,
        "diffs": _latest_file(diffs, f"depth_diffs_{symbol_fs}_*.ndjson.gz"),
        "trades": _latest_file(trades, f"trades_ws_raw_{symbol_fs}_*.ndjson.gz"),
        "live_diffs": live / "live_depth_diffs.ndjson" if live.exists() else None,
        "live_trades": live / "live_trades.ndjson" if live.exists() else None,
        "events": _latest_file(day_dir, f"events_{symbol_fs}_*.csv.gz"),
        "snapshot": _latest_file(snapshots, "snapshot_*_*.json"),
    }
    return {k: v for k, v in paths.items() if v is not None}


def _latest_file(folder: Path, pattern: str) -> Optional[Path]:
    if not folder.exists():
        return None
    candidates = sorted(folder.glob(pattern))
    if not candidates:
        return None
    return candidates[-1]
