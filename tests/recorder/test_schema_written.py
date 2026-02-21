import json
from datetime import datetime
from zoneinfo import ZoneInfo

import mm_recorder.recorder as recorder_mod
from mm_core.local_orderbook import LocalOrderBook


def test_schema_json_written(monkeypatch, tmp_path):
    monkeypatch.setenv("SYMBOL", "BTCUSDT")

    fixed_now = datetime(2025, 12, 16, 9, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)

    original_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return original_path(tmp_path / "data")
        return original_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

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
        lob = LocalOrderBook()
        lob.load_snapshot(bids=[["100", "1.0"]], asks=[["101", "1.0"]], last_update_id=10)
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        path.write_text("run_id,event_id,side,price,qty,lastUpdateId\n", encoding="utf-8")
        return lob, path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, on_status=None, **kwargs):
            self.on_open = on_open
            self.on_depth = on_depth

        def run(self):
            self.on_open()
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)

        def run_forever(self):
            return self.run()

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    recorder_mod.run_recorder()

    day_str = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    schema_path = tmp_path / "data" / "binance" / "BTCUSDT" / day_str / "schema.json"
    assert schema_path.exists()

    obj = json.loads(schema_path.read_text(encoding="utf-8"))
    assert obj["schema_version"] == recorder_mod.SCHEMA_VERSION
    assert "files" in obj
