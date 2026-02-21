import os
import gzip
import csv
from datetime import datetime
from pathlib import Path

import mm_recorder.recorder as rec


class FakeStream:
    """Minimal BinanceWSStream replacement used for unit testing."""

    def __init__(self, *, ws_url, on_depth, on_trade, on_open, on_status=None, **kwargs):
        self.ws_url = ws_url
        self.on_depth = on_depth
        self.on_trade = on_trade
        self.on_open = on_open
        self.on_status = on_status
        self.closed = False

    def close(self):
        self.closed = True

    def run(self):
        # Simulate immediate open + a single depth message.
        if self.on_open:
            self.on_open()
        # recorder.on_depth expects signature (data, recv_ms)
        self.on_depth({"e": "depthUpdate", "U": 1, "u": 1, "b": [], "a": []}, 1700000000000)
        # Return; recorder should then finalize and exit.
        return


class FakeDateTime:
    """Simple callable that returns a deterministic sequence of datetimes."""

    def __init__(self, dts):
        self._dts = list(dts)
        self._i = 0

    def __call__(self):
        if self._i >= len(self._dts):
            return self._dts[-1]
        v = self._dts[self._i]
        self._i += 1
        return v


def _read_events_csv_gz(path: Path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return list(r)


def test_recorder_emits_window_end_and_stops(tmp_path, monkeypatch):
    # Work in isolated folder
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SYMBOL", "BTCUSDT")
    monkeypatch.setenv("WINDOW_START_HHMM", "21:59")
    monkeypatch.setenv("WINDOW_END_HHMM", "22:00")
    monkeypatch.setenv("WINDOW_END_DAY_OFFSET", "0")

    # Force deterministic run_id/event_id by freezing time.time()
    monkeypatch.setattr(rec.time, "time", lambda: 1700000000.0)

    # Provide a deterministic time sequence:
    # - First call: 21:59 (so recorder starts)
    # - Subsequent calls: 22:00:01 (so hard stop triggers)
    base = datetime(2026, 1, 14, 21, 59, 0)
    after_end = datetime(2026, 1, 14, 22, 0, 1)
    fake_now = FakeDateTime([
        base,  # now
        after_end,  # on_open heartbeat check
        after_end,  # on_depth stop check
        after_end,  # final heartbeat
    ])
    monkeypatch.setattr(rec, "window_now", fake_now)

    # Avoid sleeping
    monkeypatch.setattr(rec.time, "sleep", lambda s: None)

    # Use fake stream
    monkeypatch.setattr(rec, "BinanceWSStream", FakeStream)

    # Avoid requiring python-binance for REST snapshots in unit tests.
    def _dummy_record_rest_snapshot(**kwargs):
        # Return: (lob, path, last_uid, raw_snapshot)
        from mm_core.local_orderbook import LocalOrderBook

        out = Path(kwargs["snapshots_dir"]) / "snapshot_dummy.csv.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"")

        lob = LocalOrderBook()
        lob.last_update_id = 0
        return lob, out, 0, {}

    monkeypatch.setattr(rec, "record_rest_snapshot", _dummy_record_rest_snapshot)

    # Run
    rec.run_recorder()

    # Verify day folder and events
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260114"
    assert day_dir.exists()

    events_path = day_dir / "events_BTCUSDT_20260114.csv.gz"
    assert events_path.exists()

    rows = _read_events_csv_gz(events_path)
    types = [r.get("type") for r in rows]

    # Must include window_end and run_stop to prove recorder exits its loop.
    assert "run_start" in types
    assert "window_end" in types
    assert "run_stop" in types


def test_recorder_buffered_warns_without_unbound_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SYMBOL", "BTCUSDT")
    monkeypatch.setenv("WINDOW_START_HHMM", "21:59")
    monkeypatch.setenv("WINDOW_END_HHMM", "22:00")
    monkeypatch.setenv("WINDOW_END_DAY_OFFSET", "0")

    monkeypatch.setattr(rec.time, "time", lambda: 1700000000.0)
    monkeypatch.setattr(rec.time, "sleep", lambda s: None)

    base = datetime(2026, 1, 14, 21, 59, 0)
    after_end = datetime(2026, 1, 14, 22, 0, 1)
    fake_now = FakeDateTime([base, base, after_end, after_end])
    monkeypatch.setattr(rec, "window_now", fake_now)

    monkeypatch.setattr(rec, "BinanceWSStream", FakeStream)

    class FakeResult:
        action = "buffered"
        details = "buffered_for_test"

    class FakeLob:
        last_update_id = 0

    class FakeEngine:
        def __init__(self):
            self.depth_synced = False
            self.snapshot_loaded = True
            self.lob = FakeLob()
            self.buffer = []

        def adopt_snapshot(self, lob):
            self.lob = lob

        def reset_for_resync(self):
            self.depth_synced = False
            self.buffer = []

        def feed_depth_event(self, _data):
            return FakeResult()

    class FakeAdapter:
        sync_mode = "sequence"

        def normalize_depth(self, depth):
            return depth

        def normalize_symbol(self, symbol):
            return symbol

        def ws_url(self, symbol):
            return "wss://example"

        def subscribe_messages(self, symbol, depth):
            return []

        @property
        def uses_custom_ws_messages(self):
            return False

        def parse_depth(self, data):
            from mm_recorder.exchanges.types import DepthDiff
            return DepthDiff(
                event_time_ms=int(data.get("E", 0)),
                U=int(data.get("U", 0)),
                u=int(data.get("u", 0)),
                bids=data.get("b", []),
                asks=data.get("a", []),
                raw=data,
            )

        def parse_trade(self, data):
            from mm_recorder.exchanges.types import Trade
            return Trade(
                event_time_ms=int(data.get("E", 0)),
                trade_id=int(data.get("t", 0)),
                trade_time_ms=int(data.get("T", 0)),
                price=float(data.get("p", 0)),
                qty=float(data.get("q", 0)),
                is_buyer_maker=int(data.get("m", 0)),
                side="sell" if int(data.get("m", 0)) == 1 else "buy",
                raw=data,
            )

        def create_sync_engine(self, depth):
            return FakeEngine()

    monkeypatch.setattr(rec, "get_adapter", lambda name: FakeAdapter())

    def _dummy_record_rest_snapshot(**kwargs):
        from mm_core.local_orderbook import LocalOrderBook

        out = Path(kwargs["snapshots_dir"]) / "snapshot_dummy.csv.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"")

        lob = LocalOrderBook()
        lob.last_update_id = 0
        return lob, out, 0, {}

    monkeypatch.setattr(rec, "record_rest_snapshot", _dummy_record_rest_snapshot)

    rec.run_recorder()
