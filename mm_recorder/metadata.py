from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import os
import time
from typing import Any

import requests

from mm_core.local_orderbook import get_default_tick_size


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


METADATA_TIMEOUT_S = _env_float("METADATA_TIMEOUT_S", 10.0)
METADATA_RETRY_MAX = _env_int("METADATA_RETRY_MAX", 3)
METADATA_RETRY_BACKOFF_S = _env_float("METADATA_RETRY_BACKOFF_S", 0.5)
METADATA_RETRY_BACKOFF_MAX_S = _env_float("METADATA_RETRY_BACKOFF_MAX_S", 5.0)

BINANCE_REST_BASE_URL = os.getenv("BINANCE_REST_BASE_URL", "https://api.binance.com")
KRAKEN_REST_BASE_URL = os.getenv("KRAKEN_REST_BASE_URL", "https://api.kraken.com")
BITFINEX_REST_BASE_URL = os.getenv("BITFINEX_REST_BASE_URL", "https://api.bitfinex.com")


@dataclass
class PriceTickInfo:
    exchange: str
    symbol: str
    tick_size: Decimal
    source: str
    raw: Any | None = None


def _to_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _call_with_retry(fn):
    attempts = max(1, int(METADATA_RETRY_MAX))
    backoff_s = max(0.0, float(METADATA_RETRY_BACKOFF_S))
    backoff_max_s = max(backoff_s, float(METADATA_RETRY_BACKOFF_MAX_S))
    delay = backoff_s
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            if delay > 0:
                time.sleep(delay)
                delay = min(backoff_max_s, delay * 2)
    raise last_exc


def _fetch_binance_tick_size(symbol: str) -> tuple[Decimal, Any]:
    url = f"{BINANCE_REST_BASE_URL}/api/v3/exchangeInfo"
    resp = requests.get(url, params={"symbol": symbol}, timeout=METADATA_TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    symbols = data.get("symbols") or []
    if not symbols:
        raise RuntimeError(f"Binance exchangeInfo returned no symbols for {symbol}")
    info = symbols[0]
    price_filter = None
    for flt in info.get("filters", []) or []:
        if flt.get("filterType") == "PRICE_FILTER":
            price_filter = flt
            break
    if not price_filter or "tickSize" not in price_filter:
        raise RuntimeError(f"Binance exchangeInfo missing PRICE_FILTER tickSize for {symbol}")
    tick = _to_decimal(price_filter.get("tickSize"))
    return tick, data


def _fetch_kraken_tick_size(symbol: str) -> tuple[Decimal, Any]:
    url = f"{KRAKEN_REST_BASE_URL}/0/public/AssetPairs"
    resp = requests.get(url, params={"pair": symbol}, timeout=METADATA_TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    errors = data.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken AssetPairs error for {symbol}: {errors}")
    result = data.get("result") or {}
    if not result:
        raise RuntimeError(f"Kraken AssetPairs returned no result for {symbol}")
    info = next(iter(result.values()))
    tick_raw = info.get("tick_size")
    if tick_raw is not None:
        return _to_decimal(tick_raw), data
    pair_decimals = info.get("pair_decimals")
    if pair_decimals is None:
        raise RuntimeError(f"Kraken AssetPairs missing tick_size/pair_decimals for {symbol}")
    tick = Decimal(1) / (Decimal(10) ** int(pair_decimals))
    return tick, data


def _bitfinex_pair_key(symbol: str) -> str:
    s = symbol.replace("/", "").replace("-", "").replace(":", "").strip().upper()
    if s.startswith("T") or s.startswith("F"):
        s = s[1:]
    return s.lower()


def _fetch_bitfinex_tick_size(symbol: str) -> tuple[Decimal, Any]:
    url = f"{BITFINEX_REST_BASE_URL}/v1/symbols_details"
    resp = requests.get(url, timeout=METADATA_TIMEOUT_S, headers={"User-Agent": "mm-recorder"})
    resp.raise_for_status()
    data = resp.json()
    pair_key = _bitfinex_pair_key(symbol)
    entry = None
    for row in data:
        if row.get("pair") == pair_key:
            entry = row
            break
    if entry is None:
        raise RuntimeError(f"Bitfinex symbols_details missing pair={pair_key}")
    precision = entry.get("price_precision")
    if precision is None:
        raise RuntimeError(f"Bitfinex symbols_details missing price_precision for {pair_key}")
    tick = Decimal(1) / (Decimal(10) ** int(precision))
    return tick, {"price_precision": int(precision), "raw": data}


def resolve_price_tick_size(exchange: str, symbol: str, log=None) -> PriceTickInfo:
    override = os.getenv("MM_PRICE_TICK_SIZE")
    if override:
        tick = _to_decimal(override)
        return PriceTickInfo(exchange=exchange, symbol=symbol, tick_size=tick, source="env")

    if not _env_bool("MM_METADATA_FETCH", True):
        raise RuntimeError("MM_METADATA_FETCH is disabled; set MM_PRICE_TICK_SIZE to proceed.")

    def _fetch():
        ex = (exchange or "").strip().lower()
        if ex == "binance":
            return _fetch_binance_tick_size(symbol)
        if ex == "kraken":
            return _fetch_kraken_tick_size(symbol)
        if ex == "bitfinex":
            return _fetch_bitfinex_tick_size(symbol)
        raise RuntimeError(f"Unsupported exchange for metadata: {exchange}")

    try:
        tick, raw = _call_with_retry(_fetch)
        info = PriceTickInfo(exchange=exchange, symbol=symbol, tick_size=_to_decimal(tick), source="metadata", raw=raw)
        if log is not None and (exchange or "").strip().lower() == "bitfinex":
            precision = None
            if isinstance(raw, dict):
                precision = raw.get("price_precision")
            if precision is not None:
                log.warning(
                    "Bitfinex does not publish a fixed tick size; derived from price_precision=%s (significant digits).",
                    precision,
                )
            else:
                log.warning(
                    "Bitfinex does not publish a fixed tick size; derived from price_precision (significant digits)."
                )
        return info
    except Exception as exc:
        if _env_bool("MM_METADATA_STRICT", True):
            raise
        if log is not None:
            log.warning("Metadata fetch failed for %s %s; falling back to default tick size: %s", exchange, symbol, exc)
        return PriceTickInfo(
            exchange=exchange,
            symbol=symbol,
            tick_size=get_default_tick_size(),
            source="default",
        )
