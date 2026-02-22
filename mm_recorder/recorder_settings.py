from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y")


DECIMALS = 8
# Stored top-N width in orderbook_ws_depth_*.csv.gz. Kept per-exchange because
# Kraken/Binance benefit from deeper volatility analysis while Bitfinex sync is
# currently maintained/validated at top25.
DEPTH_LEVELS_DEFAULT = _env_int("DEPTH_LEVELS", 20)
DEPTH_LEVELS_BINANCE = _env_int("DEPTH_LEVELS_BINANCE", 100)
DEPTH_LEVELS_KRAKEN = _env_int("DEPTH_LEVELS_KRAKEN", 100)
DEPTH_LEVELS_BITFINEX = _env_int("DEPTH_LEVELS_BITFINEX", 25)


def depth_levels_for_exchange(exchange: str) -> int:
    ex = (exchange or "").strip().lower()
    if ex == "binance":
        return max(1, int(DEPTH_LEVELS_BINANCE))
    if ex == "kraken":
        return max(1, int(DEPTH_LEVELS_KRAKEN))
    if ex == "bitfinex":
        return max(1, int(DEPTH_LEVELS_BITFINEX))
    return max(1, int(DEPTH_LEVELS_DEFAULT))

HEARTBEAT_SEC = 30
SYNC_WARN_AFTER_SEC = 10
MAX_BUFFER_WARN = 5000
SNAPSHOT_LIMIT = 1000
ORDERBOOK_BUFFER_ROWS = 500
TRADES_BUFFER_ROWS = 1000
BUFFER_FLUSH_INTERVAL_SEC = 1.0

# WS keepalive/reconnect
WS_PING_INTERVAL_S = _env_int("WS_PING_INTERVAL_S", 20)
WS_PING_TIMEOUT_S = _env_int("WS_PING_TIMEOUT_S", 60)
WS_RECONNECT_BACKOFF_S = _env_float("WS_RECONNECT_BACKOFF_S", 1.0)
WS_RECONNECT_BACKOFF_MAX_S = _env_float("WS_RECONNECT_BACKOFF_MAX_S", 30.0)
WS_MAX_SESSION_S = _env_float("WS_MAX_SESSION_S", float(23 * 3600 + 50 * 60))
WS_OPEN_TIMEOUT_S = _env_float("WS_OPEN_TIMEOUT_S", 10.0)
WS_NO_DATA_WARN_S = _env_float("WS_NO_DATA_WARN_S", 10.0)

# TLS verification should remain enabled by default.
INSECURE_TLS = _env_bool("INSECURE_TLS", False)

# If True, write raw WS depth diffs for production-faithful replay
STORE_DEPTH_DIFFS = True
LIVE_STREAM_ENABLED = _env_bool("LIVE_STREAM", True)
LIVE_STREAM_ROTATE_S = _env_float("LIVE_STREAM_ROTATE_S", 60.0)
LIVE_STREAM_RETENTION_S = _env_float("LIVE_STREAM_RETENTION_S", float(60 * 60))
