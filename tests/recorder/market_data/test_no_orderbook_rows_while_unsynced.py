import csv
from datetime import datetime
from zoneinfo import ZoneInfo
import gzip

import mm_recorder.recorder as recorder_mod


class DummyLob:
    def __init__(self, last_update_id=10):
        self.last_update_id = last_update_id
        self.bids = {100.0: 1.0}
        self.asks = {101.0: 1.0}

    def apply_diff(self, U, u, bids, asks):
        # it doesn't matter; we never bridge in this test
        self.last_update_id = self.last_update_id
        return True

    def top_n(self, n):
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks


def test_no_orderbook_rows_until_synced(monkeypatch, tmp_path):
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)
    monkeypatch.setenv("SYMBOL", "ETHUSDT")

    orig_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return orig_path(tmp_path / "data")
        return orig_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    # Snapshot returns lastUpdateId=10
    def fake_record_rest_snapshot(client, symbol, day_dir, snapshots_dir, limit, run_id, event_id, tag, decimals=8):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        snap_path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        snap_path.write_text("dummy\n", encoding="utf-8")
        return DummyLob(last_update_id=10), snap_path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # WS emits depth updates that NEVER satisfy bridge condition for lu=10:
    # bridge requires U <= 11 <= u; we'll send U=50,u=51 always.
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls, **kwargs):
            self.on_open = on_open
            self.on_depth = on_depth

        def run_forever(self):
            self.on_open()
            for i in range(5):
                self.on_depth({"e": "depthUpdate", "E": i + 1, "U": 50, "u": 51, "b": [], "a": []}, 100 + i)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)
    recorder_mod.run_recorder()

    symbol = "ETHUSDT"
    day = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    day_dir = tmp_path / "data" / "binance" / symbol / day
    orderbook_path = day_dir / f"orderbook_ws_depth_{symbol}_{day}.csv.gz"

    rows = list(csv.reader(gzip.open(orderbook_path, 'rt', encoding='utf-8', newline='')))
    # header only (no data rows) because never synced
    assert len(rows) == 1
    assert rows[0][:5] == ["event_time_ms", "recv_time_ms", "recv_seq", "run_id", "epoch_id"]
