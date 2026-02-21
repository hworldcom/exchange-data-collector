# tests/_paths.py

from pathlib import Path
import os

from mm_core.symbols import symbol_fs as symbol_fs_fn


def day_str(recorder_mod) -> str:
    now = recorder_mod.window_now()
    window_start, window_end = recorder_mod.compute_window(now)
    if now < window_start:
        prev_start = window_start - recorder_mod.timedelta(days=1)
        prev_end = window_end - recorder_mod.timedelta(days=1)
        if now <= prev_end:
            window_start = prev_start
    return window_start.strftime("%Y%m%d")


def _exchange(recorder_mod) -> str:
    return os.getenv("EXCHANGE", "binance").strip().lower() or "binance"


def _symbol_fs(recorder_mod, symbol: str) -> str:
    adapter = recorder_mod.get_adapter(_exchange(recorder_mod))
    normalized = adapter.normalize_symbol(symbol)
    return symbol_fs_fn(normalized)


def day_dir(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    exchange = _exchange(recorder_mod)
    symbol_fs = _symbol_fs(recorder_mod, symbol)
    return tmp_path / "data" / exchange / symbol_fs / day_str(recorder_mod)


def orderbook_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    symbol_fs = _symbol_fs(recorder_mod, symbol)
    return d / f"orderbook_ws_depth_{symbol_fs}_{day}.csv.gz"


def trades_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    symbol_fs = _symbol_fs(recorder_mod, symbol)
    return d / f"trades_ws_{symbol_fs}_{day}.csv.gz"


def gaps_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    symbol_fs = _symbol_fs(recorder_mod, symbol)
    return d / f"gaps_{symbol_fs}_{day}.csv.gz"


def events_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    symbol_fs = _symbol_fs(recorder_mod, symbol)
    return d / f"events_{symbol_fs}_{day}.csv.gz"
