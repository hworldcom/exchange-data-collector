from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import mm_api.relay as relay


class StopStream(Exception):
    pass


def _write_snapshot(path: Path, bids, asks) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"bids": bids, "asks": asks}
    path.write_text(json.dumps(payload), encoding="utf-8")


def _make_day(tmp_path: Path, day: str, bids, asks) -> tuple[Path, Path]:
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / day
    snapshot_path = day_dir / "snapshots" / "snapshot_000001_initial.json"
    _write_snapshot(snapshot_path, bids=bids, asks=asks)
    return day_dir, snapshot_path


def _fast_sleep(_: float) -> asyncio.Future:
    async def _noop():
        return None

    return asyncio.ensure_future(_noop())


def test_relay_rollover_snapshot_payload_is_dict(tmp_path, monkeypatch):
    day1_dir, snap1 = _make_day(tmp_path, "20260101", bids=[[100, 1]], asks=[[101, 2]])
    day2_dir, snap2 = _make_day(tmp_path, "20260102", bids=[[90, 1]], asks=[[91, 2]])

    calls = {"n": 0}

    def fake_resolve(exchange: str, symbol: str):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"day_dir": day1_dir, "snapshot": snap1}
        return {"day_dir": day2_dir, "snapshot": snap2}

    monkeypatch.setattr(relay, "resolve_latest_paths", fake_resolve)
    monkeypatch.setattr(relay, "POLL_INTERVAL_S", 0.0)
    monkeypatch.setattr(relay.asyncio, "sleep", _fast_sleep)

    class FakeWS:
        def __init__(self):
            self.messages = []
            self.snapshot_count = 0

        async def send(self, payload: str) -> None:
            msg = json.loads(payload)
            self.messages.append(msg)
            if msg.get("type") == "snapshot":
                self.snapshot_count += 1
                if self.snapshot_count >= 2:
                    raise StopStream()

    ws = FakeWS()

    with pytest.raises(StopStream):
        asyncio.run(relay._stream_loop(ws, "binance", "BTCUSDT", "tail"))

    snapshots = [m for m in ws.messages if m.get("type") == "snapshot"]
    assert len(snapshots) >= 2
    assert isinstance(snapshots[1]["data"], dict)
    assert snapshots[1]["data"]["bids"][0][0] == 90


def test_relay_rollover_resets_book_from_snapshot(tmp_path, monkeypatch):
    day1_dir, snap1 = _make_day(tmp_path, "20260101", bids=[[100, 1]], asks=[[101, 2]])
    day2_dir, snap2 = _make_day(tmp_path, "20260102", bids=[[90, 1]], asks=[[91, 2]])

    calls = {"n": 0}

    def fake_resolve(exchange: str, symbol: str):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"day_dir": day1_dir, "snapshot": snap1}
        return {"day_dir": day2_dir, "snapshot": snap2}

    monkeypatch.setattr(relay, "resolve_latest_paths", fake_resolve)
    monkeypatch.setattr(relay, "POLL_INTERVAL_S", 0.0)
    monkeypatch.setattr(relay, "LEVELS_INTERVAL_S", 0.0)
    monkeypatch.setattr(relay, "LEVELS_N", 1)
    monkeypatch.setattr(relay.asyncio, "sleep", _fast_sleep)

    class FakeWS:
        def __init__(self):
            self.snapshot_count = 0

        async def send(self, payload: str) -> None:
            msg = json.loads(payload)
            if msg.get("type") == "snapshot":
                self.snapshot_count += 1
            if self.snapshot_count >= 2 and msg.get("type") == "levels":
                bids = msg["data"]["bids"]
                asks = msg["data"]["asks"]
                assert bids[0][0] == 90.0
                assert asks[0][0] == 91.0
                raise StopStream()

    ws = FakeWS()

    with pytest.raises(StopStream):
        asyncio.run(relay._stream_loop(ws, "binance", "BTCUSDT", "tail"))
