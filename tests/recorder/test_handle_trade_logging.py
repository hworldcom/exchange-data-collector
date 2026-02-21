from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import mm_recorder.recorder as recorder_mod


class _FailingCSVWriter:
    def __init__(self, path, header=None, **kwargs) -> None:
        self.header = list(header or [])
        self.path = Path(path)

    def ensure_file(self) -> None:
        return None

    def write_row(self, _row) -> None:
        if "trade_id" in self.header:
            raise RuntimeError("trade write failure")

    def close(self) -> None:
        return None


class _FakeStream:
    def __init__(self, ws_url, on_depth, on_trade, on_open, on_status=None, **kwargs):
        self.on_open = on_open
        self.on_trade = on_trade

    def run(self):
        if self.on_open:
            self.on_open()
        self.on_trade({"e": "trade", "E": 1, "t": 1, "T": 1, "p": "100", "q": "0.1", "m": 0}, 111)

    def close(self):
        return None


def test_trade_error_logging_does_not_raise_name_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SYMBOL", "BTCUSDT")
    monkeypatch.setenv("WINDOW_START_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_HHMM", "23:59")
    monkeypatch.setenv("WINDOW_END_DAY_OFFSET", "0")
    monkeypatch.setenv("LIVE_STREAM", "0")

    fixed_now = datetime(2026, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)
    monkeypatch.setattr(recorder_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1700000000.0)
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")
    monkeypatch.setattr(recorder_mod, "BufferedCSVWriter", _FailingCSVWriter)
    monkeypatch.setattr(recorder_mod, "BinanceWSStream", _FakeStream)

    def _dummy_record_rest_snapshot(**kwargs):
        from mm_core.local_orderbook import LocalOrderBook

        snapshots_dir = kwargs["snapshots_dir"]
        event_id = kwargs["event_id"]
        tag = kwargs["tag"]
        snap_path = Path(snapshots_dir) / f"snapshot_{event_id:06d}_{tag}.csv"
        snap_path.parent.mkdir(parents=True, exist_ok=True)
        snap_path.write_text(
            "run_id,event_id,side,price,qty,lastUpdateId\n"
            "1,1,bid,100,1,10\n"
            "1,1,ask,101,1,10\n",
            encoding="utf-8",
        )

        lob = LocalOrderBook()
        lob.load_snapshot(bids=[["100", "1"]], asks=[["101", "1"]], last_update_id=10)
        return lob, snap_path, 10, {"bids": [["100", "1"]], "asks": [["101", "1"]], "lastUpdateId": 10}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", _dummy_record_rest_snapshot)

    recorder_mod.run_recorder()
