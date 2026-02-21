import csv
from datetime import datetime
from zoneinfo import ZoneInfo
import gzip

import mm_recorder.recorder as recorder_mod
from tests._paths import orderbook_path as get_orderbook_path
from tests._paths import trades_path as get_trades_path
from tests._paths import events_path as get_events_path
from tests._paths import day_str as get_day_str


class DummyLob:
    def __init__(self, last_update_id=10):
        self.last_update_id = last_update_id
        self.bids = {100.0: 1.0}
        self.asks = {101.0: 1.0}

    def apply_diff(self, U, u, bids, asks):
        self.last_update_id = int(u)
        return True

    def top_n(self, n):
        bids = sorted(self.bids.items(), reverse=True)[:n]
        asks = sorted(self.asks.items())[:n]
        return bids, asks


def test_headers_written_once_across_restarts(monkeypatch, tmp_path):
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

    def fake_record_rest_snapshot(client, symbol, day_dir, snapshots_dir, limit, run_id, event_id, tag, decimals=8):
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        snap_path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
        snap_path.write_text("dummy\n", encoding="utf-8")
        return DummyLob(last_update_id=10), snap_path, 10, {}

    monkeypatch.setattr(recorder_mod, "record_rest_snapshot", fake_record_rest_snapshot)

    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, insecure_tls, **kwargs):
            self.on_open = on_open
            self.on_depth = on_depth

        def run_forever(self):
            self.on_open()
            self.on_depth({"e": "depthUpdate", "E": 1, "U": 10, "u": 11, "b": [], "a": []}, 111)

        def close(self):
            pass

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    date = get_day_str(recorder_mod)

    # Run 1
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)
    recorder_mod.run_recorder()

    # Run 2
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 2.0)
    recorder_mod.run_recorder()

    day_dir = tmp_path / "data" / "binance" / "ETHUSDT" / date

    symbol = "ETHUSDT"
    orderbook_path = get_orderbook_path(tmp_path, recorder_mod, symbol)
    trades_path = get_trades_path(tmp_path, recorder_mod, symbol)
    events_path = get_events_path(tmp_path, recorder_mod, symbol)

    # Count header occurrences in each CSV (header row repeated indicates bug)
    def header_count(path, expected_header):
        rows = list(csv.reader(gzip.open(path, 'rt', encoding='utf-8', newline='')))
        return sum(1 for r in rows if r == expected_header)

    ob_header = ["event_time_ms", "recv_time_ms", "recv_seq", "run_id", "epoch_id"]
    tr_header = [
        "event_time_ms",
        "recv_time_ms",
        "recv_seq",
        "run_id",
        "trade_id",
        "trade_time_ms",
        "price",
        "qty",
        "is_buyer_maker",
        "side",
        "ord_type",
        "exchange",
        "symbol",
    ]
    ev_header = ["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"]

    # orderbook header has extra columns beyond the first 4; compare prefix for robustness
    ob_rows = list(csv.reader(gzip.open(orderbook_path, 'rt', encoding='utf-8', newline='')))
    assert ob_rows[0][:5] == ob_header
    # ensure header row appears only once (exact match on full row)
    assert header_count(orderbook_path, ob_rows[0]) == 1

    assert header_count(trades_path, tr_header) == 1
    assert header_count(events_path, ev_header) == 1
