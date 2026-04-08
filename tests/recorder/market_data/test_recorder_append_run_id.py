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


def test_restart_keeps_recv_seq_monotonic_across_runs(monkeypatch, tmp_path):
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)

    symbol = "ETHUSDT"
    monkeypatch.setenv("SYMBOL", symbol)

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
        snap_path.write_text(
            "run_id,event_id,side,price,qty,lastUpdateId\n"
            "1,1,bid,100,1,10\n"
            "1,1,ask,101,1,10\n",
            encoding="utf-8",
        )
        return DummyLob(last_update_id=10), snap_path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, on_status, insecure_tls, **kwargs):
            self.on_open = on_open
            self.on_depth = on_depth
            self.on_trade = on_trade
            self.on_status = on_status

        def run(self):
            self.on_open()
            self.on_status("status", {"ok": True})
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)
            self.on_trade({"e": "trade", "E": 2, "t": 1, "T": 2, "p": "100", "q": "0.1", "m": 0}, 112)

        def run_forever(self):
            return self.run()

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    clock = {"base": 1.0, "tick": 0}

    def fake_time():
        value = clock["base"] + (clock["tick"] * 0.001)
        clock["tick"] += 1
        return value

    monkeypatch.setattr(recorder_mod.time, "time", fake_time)
    recorder_mod.run_recorder()
    clock["base"] = 2.0
    clock["tick"] = 0
    recorder_mod.run_recorder()

    day = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    base = tmp_path / "data" / "binance" / symbol / day

    events_path = base / f"events_{symbol}_{day}.csv.gz"
    trades_path = base / f"trades_ws_{symbol}_{day}.csv.gz"
    diffs_path = base / "diffs" / f"depth_diffs_{symbol}_{day}.ndjson.gz"

    event_rows = list(csv.DictReader(gzip.open(events_path, "rt", encoding="utf-8", newline="")))
    trade_rows = list(csv.DictReader(gzip.open(trades_path, "rt", encoding="utf-8", newline="")))
    diff_rows = [json.loads(line) for line in gzip.open(diffs_path, "rt", encoding="utf-8") if line.strip()]

    event_seqs = [int(row["recv_seq"]) for row in event_rows]
    trade_seqs = [int(row["recv_seq"]) for row in trade_rows]
    diff_seqs = [int(row["recv_seq"]) for row in diff_rows]

    all_seqs = event_seqs + trade_seqs + diff_seqs
    assert len(all_seqs) == len(set(all_seqs))

    run_start_rows = [row for row in event_rows if row["type"] == "run_start"]
    assert len(run_start_rows) == 2
    first_run_id = int(run_start_rows[0]["run_id"])
    second_run_id = int(run_start_rows[1]["run_id"])
    first_run_start_seq = int(run_start_rows[0]["recv_seq"])
    second_run_start_seq = int(run_start_rows[1]["recv_seq"])
    first_run_max_seq = max(
        int(row["recv_seq"])
        for row in event_rows + trade_rows
        if int(row["run_id"]) == first_run_id
    )
    first_run_max_seq = max(
        first_run_max_seq,
        max(int(row["recv_seq"]) for row in diff_rows[:1]),
    )

    assert first_run_id != second_run_id
    assert second_run_start_seq > first_run_start_seq
    assert second_run_start_seq > first_run_max_seq
