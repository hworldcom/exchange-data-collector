import csv
import gzip
import json
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


def _read_events_recv_seq(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', newline='') as f:
        rows = list(csv.DictReader(f))
    return [int(r["recv_seq"]) for r in rows]


def _read_trades_recv_seq(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', newline='') as f:
        rows = list(csv.DictReader(f))
    return [int(r["recv_seq"]) for r in rows]


def _read_diffs_recv_seq(path):
    out = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            out.append(int(json.loads(line)["recv_seq"]))
    return out


def test_global_recv_seq_is_unique_across_message_types(monkeypatch, tmp_path):
    """Recorder should emit a single global recv_seq across depth, trade, and events."""

    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)

    symbol = "BTCUSDT"
    monkeypatch.setenv("SYMBOL", symbol)

    # Redirect data/ into tmp_path
    orig_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return orig_path(tmp_path / "data")
        return orig_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    def fake_record_rest_snapshot(client, symbol, day_dir, snapshots_dir, limit, run_id, event_id, tag, decimals=8):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        snap_path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        # Minimal snapshot schema used by replay loader
        snap_path.write_text(
            "run_id,event_id,side,price,qty,lastUpdateId\n" "1,1,bid,100,1,10\n" "1,1,ask,101,1,10\n",
            encoding="utf-8",
        )
        return DummyLob(last_update_id=10), snap_path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # Make time.time deterministic but changing, so recv_time_ms is not constant.
    t = {"v": 1.0}

    def fake_time():
        t["v"] += 0.001
        return t["v"]

    monkeypatch.setattr(recorder_mod.time, "time", fake_time)

    # Fake stream emits one depth and one trade.
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, on_status, insecure_tls, **kwargs):
            self.on_depth = on_depth
            self.on_trade = on_trade
            self.on_open = on_open
            self.on_status = on_status

        def run(self):
            self.on_open()
            # Some streams emit status events; include one to ensure it's sequenced.
            self.on_status("status", {"ok": True})
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            self.on_trade({"e": "trade", "E": 2, "t": 1, "T": 2, "p": "100", "q": "0.1", "m": 0}, 112)

        # Older interface
        def run_forever(self):
            return self.run()

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    recorder_mod.run_recorder()

    day = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    base = tmp_path / "data" / "binance" / symbol / day
    events_path = base / f"events_{symbol}_{day}.csv.gz"
    trades_path = base / f"trades_ws_{symbol}_{day}.csv.gz"
    diffs_path = base / "diffs" / f"depth_diffs_{symbol}_{day}.ndjson.gz"

    ev_seq = _read_events_recv_seq(events_path)
    tr_seq = _read_trades_recv_seq(trades_path)
    dd_seq = _read_diffs_recv_seq(diffs_path)

    all_seq = ev_seq + tr_seq + dd_seq
    assert len(all_seq) > 0
    # Uniqueness is the key property of a global receive sequence.
    assert len(set(all_seq)) == len(all_seq)
