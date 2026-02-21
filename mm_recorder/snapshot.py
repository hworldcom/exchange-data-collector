import csv
import os
import time
from pathlib import Path

import json
import requests

from mm_core.local_orderbook import LocalOrderBook

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


BINANCE_REST_BASE_URL = os.getenv("BINANCE_REST_BASE_URL", "https://api.binance.com")
SNAPSHOT_TIMEOUT_S = _env_float("SNAPSHOT_TIMEOUT_S", 10.0)
SNAPSHOT_RETRY_MAX = _env_int("SNAPSHOT_RETRY_MAX", 3)
SNAPSHOT_RETRY_BACKOFF_S = _env_float("SNAPSHOT_RETRY_BACKOFF_S", 0.5)
SNAPSHOT_RETRY_BACKOFF_MAX_S = _env_float("SNAPSHOT_RETRY_BACKOFF_MAX_S", 5.0)


class BinanceRestClient:
    def __init__(self, base_url: str | None = None, timeout_s: float | None = None) -> None:
        self.base_url = base_url or BINANCE_REST_BASE_URL
        self.timeout_s = SNAPSHOT_TIMEOUT_S if timeout_s is None else float(timeout_s)

    def get_order_book(self, symbol: str, limit: int) -> dict:
        url = f"{self.base_url}/api/v3/depth"
        resp = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()


def make_rest_client(exchange: str):
    if (exchange or "").strip().lower() == "binance":
        return BinanceRestClient()
    return None


def _call_with_retry(fn):
    attempts = max(1, int(SNAPSHOT_RETRY_MAX))
    backoff_s = max(0.0, float(SNAPSHOT_RETRY_BACKOFF_S))
    backoff_max_s = max(backoff_s, float(SNAPSHOT_RETRY_BACKOFF_MAX_S))
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


def write_snapshot_csv(
    *,
    path: Path,
    run_id: int,
    event_id: int,
    bids: list[list[str]],
    asks: list[list[str]],
    last_update_id: int,
    checksum: int | None = None,
    decimals: int = 8,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        header = ["run_id", "event_id", "side", "price", "qty", "lastUpdateId"]
        if checksum is not None:
            header.append("checksum")
        w.writerow(header)
        for p, q in sorted(((float(b[0]), float(b[1])) for b in bids), reverse=True):
            row = [run_id, event_id, "bid", f"{p:.{decimals}f}", f"{q:.{decimals}f}", last_update_id]
            if checksum is not None:
                row.append(int(checksum))
            w.writerow(row)
        for p, q in sorted(((float(a[0]), float(a[1])) for a in asks)):
            row = [run_id, event_id, "ask", f"{p:.{decimals}f}", f"{q:.{decimals}f}", last_update_id]
            if checksum is not None:
                row.append(int(checksum))
            w.writerow(row)


def write_snapshot_json(*, path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, default=str))


def _validate_snapshot_payload(snap: dict) -> tuple[list, list, int]:
    if not isinstance(snap, dict):
        raise ValueError("snapshot payload must be a dict")
    if "bids" not in snap or "asks" not in snap or "lastUpdateId" not in snap:
        raise ValueError("snapshot payload missing required keys")
    bids = snap.get("bids")
    asks = snap.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise ValueError("snapshot bids/asks must be lists")
    try:
        last_update_id = int(snap.get("lastUpdateId"))
    except Exception as exc:
        raise ValueError("snapshot lastUpdateId must be int-like") from exc
    return bids, asks, last_update_id


def record_rest_snapshot(
    client,
    symbol: str,
    day_dir: Path,
    snapshots_dir: Path,
    limit: int,
    run_id: int,
    event_id: int,
    tag: str,
    decimals: int = 8,
) -> tuple[LocalOrderBook, Path, int, dict]:
    """Fetch a REST snapshot and persist it for audit/replay.

    Snapshot path naming is event-driven so multiple snapshots per day (resyncs) do not overwrite.
    """
    if client is None:
        raise RuntimeError(
            "REST snapshot requires a client; configure one for the target exchange."
        )
    try:
        snap = _call_with_retry(lambda: client.get_order_book(symbol=symbol, limit=limit))
    except Exception as exc:
        raise RuntimeError(f"REST snapshot via client failed: {exc}") from exc
    try:
        bids, asks, last_update_id = _validate_snapshot_payload(snap)
    except Exception as exc:
        raise RuntimeError(f"Invalid snapshot payload: {exc}") from exc

    lob = LocalOrderBook()
    lob.load_snapshot(bids=bids, asks=asks, last_update_id=last_update_id)

    path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
    bids_levels, asks_levels = lob.levels()
    write_snapshot_csv(
        path=path,
        run_id=run_id,
        event_id=event_id,
        bids=[[str(p), str(q)] for p, q in bids_levels],
        asks=[[str(p), str(q)] for p, q in asks_levels],
        last_update_id=last_update_id,
        decimals=decimals,
    )

    return lob, path, last_update_id, snap
