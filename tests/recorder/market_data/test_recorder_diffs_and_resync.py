import csv
import gzip
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import mm_recorder.recorder as recorder_mod
from mm_core.local_orderbook import LocalOrderBook


def test_recorder_writes_diffs_and_resync(monkeypatch, tmp_path):
    # --- Fix symbol & Berlin time (within window) ---
    monkeypatch.setenv("SYMBOL", "BTCUSDT")

    fixed_now = datetime(2025, 12, 16, 9, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)

    # --- Redirect Path("data") -> tmp_path/data ---
    original_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return original_path(tmp_path / "data")
        return original_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)

    # --- Avoid real logging config side effects ---
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    # --- Mock REST snapshot to produce deterministic lastUpdateId and capture tags ---
    calls = {"tags": []}

    def fake_record_rest_snapshot(
        client,
        symbol,
        day_dir,
        snapshots_dir,
        limit,
        run_id,
        event_id,
        tag,
        decimals=8,
    ):
        calls["tags"].append(tag)

        # Different snapshot ids: initial is old (forces bridge_impossible), resync is newer
        last_uid = 10 if tag == "initial" else 20

        lob = LocalOrderBook()
        lob.load_snapshot(bids=[["100", "1.0"]], asks=[["101", "1.0"]], last_update_id=last_uid)

        snapshots_dir.mkdir(parents=True, exist_ok=True)
        path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        path.write_text("run_id,event_id,side,price,qty,lastUpdateId\n", encoding="utf-8")
        return lob, path, last_uid, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    # --- Fake stream that triggers:
    # 1) initial snapshot
    # 2) depth event with U too high => bridge_impossible => resync
    # 3) bridging event for resync snapshot => sync
    # 4) one more sequential event => applied
    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls, **kwargs):
            self.on_open = on_open
            self.on_depth = on_depth
            self.on_trade = on_trade

        def run_forever(self):
            self.on_open()

            # (2) bridge impossible for initial lastUpdateId=10: U=20 > 11
            self.on_depth({"E": 1, "U": 20, "u": 20, "b": [], "a": []}, 111)

            # After resync snapshot lastUpdateId=20, bridge with U=20 u=21
            self.on_depth({"E": 2, "U": 20, "u": 21, "b": [], "a": []}, 222)

            # Sequential apply
            self.on_depth({"E": 3, "U": 22, "u": 22, "b": [], "a": []}, 333)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    # --- Run recorder ---
    recorder_mod.run_recorder()

    # --- Assert snapshot tags ---
    assert "initial" in calls["tags"]
    assert any(t.startswith("resync_") for t in calls["tags"])

    # --- Assert outputs exist ---
    day_str = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    base_dir = tmp_path / "data" / "binance" / "BTCUSDT" / day_str

    # Diffs file
    diffs = list((base_dir / "diffs").glob("depth_diffs_BTCUSDT_*.ndjson.gz"))
    assert len(diffs) == 1

    with gzip.open(diffs[0], "rt", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    assert len(lines) >= 2  # should have written all depth events

    # Gaps file
    gaps = list(base_dir.glob(f"gaps_BTCUSDT_{day_str}.csv.gz"))
    assert len(gaps) == 1

    with gzip.open(gaps[0], 'rt', encoding='utf-8', newline='') as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"]
    # Header: recv_time_ms, recv_seq, run_id, epoch_id, event, details
    events = [r[4] for r in rows[1:]]
    assert "resync_start" in events
    assert "resync_done" in events
