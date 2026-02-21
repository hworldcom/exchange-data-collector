import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from tests._paths import orderbook_path as get_orderbook_path
from tests._paths import events_path as get_events_path
import gzip

import mm_recorder.recorder as recorder_mod


class DummyLob:
    def __init__(self, last_update_id=10):
        self.last_update_id = last_update_id
        self.bids = {100.0: 1.0}
        self.asks = {101.0: 1.0}

    def apply_diff(self, U, u, bids, asks):
        U = int(U); u = int(u)
        # simulate normal sequential behavior: reject gaps
        if self.last_update_id is None:
            return False
        if u <= self.last_update_id:
            return True
        if U > self.last_update_id + 1:
            return False
        self.last_update_id = u
        return True

    def top_n(self, n):
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks


def test_epoch_id_increments_after_resync(monkeypatch, tmp_path):
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)
    monkeypatch.setenv("SYMBOL", "ETHUSDT")

    # Redirect data/
    orig_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return orig_path(tmp_path / "data")
        return orig_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)

    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    # Snapshot writer always returns lastUpdateId=10 (fine for deterministic sync)
    def fake_record_rest_snapshot(client, symbol, day_dir, snapshots_dir, limit, run_id, event_id, tag, decimals=8):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        snap_path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        snap_path.write_text("dummy\n", encoding="utf-8")
        return DummyLob(last_update_id=10), snap_path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # Fake WS: sync -> applied -> gap -> resync -> sync again -> applied
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls, **kwargs):
            self.on_open = on_open
            self.on_depth = on_depth

        def run_forever(self):
            self.on_open()

            # initial sync bridge (epoch becomes 1)
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            self.on_depth({"e": "depthUpdate", "E": 2, "U": 12, "u": 12, "b": [], "a": []}, 222)

            # create a gap (forces resync)
            self.on_depth({"e": "depthUpdate", "E": 3, "U": 100, "u": 100, "b": [], "a": []}, 333)

            # after resync snapshot, we need to bridge again
            self.on_depth({"e": "depthUpdate", "E": 4, "U": 10, "u": 11, "b": [], "a": []}, 444)
            self.on_depth({"e": "depthUpdate", "E": 5, "U": 12, "u": 12, "b": [], "a": []}, 555)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)
    recorder_mod.run_recorder()

    symbol = "ETHUSDT"
    orderbook_path = get_orderbook_path(tmp_path, recorder_mod, symbol)
    events_path = get_events_path(tmp_path, recorder_mod, symbol)

    # Orderbook should contain at least one row in epoch 1 and epoch 2
    ob_rows = list(csv.reader(gzip.open(orderbook_path, 'rt', encoding='utf-8', newline='')))
    header = ob_rows[0]
    epoch_idx = header.index("epoch_id")
    epochs = [int(r[epoch_idx]) for r in ob_rows[1:]]
    assert 0 in epochs
    assert 1 in epochs
    assert 2 not in epochs

    # Events should contain resync markers and show epoch progress
    ev_rows = list(csv.reader(gzip.open(events_path, 'rt', encoding='utf-8', newline='')))
    # Header: event_id, recv_time_ms, recv_seq, run_id, type, epoch_id, details_json
    types = [r[4] for r in ev_rows[1:]]
    assert "resync_start" in types
    assert "resync_done" in types
