import csv
import gzip
from datetime import datetime
from zoneinfo import ZoneInfo

import mm_recorder.recorder as recorder_mod


class DummyLob:
    def __init__(self, last_update_id=10):
        self.last_update_id = last_update_id
        self.bids = {100.0: 1.0}
        self.asks = {101.0: 1.0}

    def apply_diff(self, U, u, bids, asks):
        self.last_update_id = u
        return True

    def top_n(self, n):
        return sorted(self.bids.items(), reverse=True)[:n], sorted(self.asks.items())[:n]


def test_appends_and_run_id_changes(monkeypatch, tmp_path):
    # Fixed Berlin time inside recording window
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)

    symbol = "ETHUSDT"
    monkeypatch.setenv("SYMBOL", symbol)

    # Redirect data/ into tmp_path
    orig_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return orig_path(tmp_path / "data")
        return orig_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)

    # Avoid touching real logs
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    def fake_record_rest_snapshot(client, symbol, day_dir, snapshots_dir, limit, run_id, event_id, tag, decimals=8):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        snap_path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        snap_path.write_text("dummy\n", encoding="utf-8")
        return DummyLob(last_update_id=10), snap_path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # Fake WS stream: open, then emit 2 depth events
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls, **kwargs):
            self.on_depth = on_depth
            self.on_open = on_open

        def run_forever(self):
            self.on_open()
            # bridge
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            # applied
            self.on_depth({"e": "depthUpdate", "E": 2, "U": 12, "u": 12, "b": [], "a": []}, 222)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    # Run 1: run_id uses time.time() * 1000
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)  # run_id=1000
    recorder_mod.run_recorder()

    # Run 2: different run_id
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 2.0)  # run_id=2000
    recorder_mod.run_recorder()

    day = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    ob_path = tmp_path / "data" / "binance" / symbol / day / f"orderbook_ws_depth_{symbol}_{day}.csv.gz"

    with gzip.open(ob_path, 'rt', encoding='utf-8', newline='') as f:
        rows = list(csv.reader(f))

    # Header begins with event_time_ms, recv_time_ms, recv_seq, run_id, epoch_id
    assert rows[0][:5] == ["event_time_ms", "recv_time_ms", "recv_seq", "run_id", "epoch_id"]

    # We expect data rows from both runs (append mode)
    run_ids = [r[3] for r in rows[1:] if r and r[3].isdigit()]
    assert "1000" in run_ids
    assert "2000" in run_ids
    assert run_ids[0] != run_ids[-1]
