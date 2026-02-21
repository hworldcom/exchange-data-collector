import csv
import gzip
from datetime import datetime
from zoneinfo import ZoneInfo

import mm_recorder.recorder as recorder_mod


def test_kraken_checksum_resync(monkeypatch, tmp_path):
    fixed_now = datetime(2025, 12, 15, 12, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    monkeypatch.setattr(recorder_mod, "window_now", lambda: fixed_now)
    monkeypatch.setenv("EXCHANGE", "kraken")
    monkeypatch.setenv("SYMBOL", "BTC/USD")

    orig_path = recorder_mod.Path

    def PatchedPath(p):
        if p == "data":
            return orig_path(tmp_path / "data")
        return orig_path(p)

    monkeypatch.setattr(recorder_mod, "Path", PatchedPath)
    monkeypatch.setattr(recorder_mod, "setup_logging", lambda *args, **kwargs: tmp_path / "log.txt")

    class FakeStream:
        def __init__(self, ws_url, on_depth, on_trade, on_open, on_message, on_status=None, **kwargs):
            self.on_open = on_open
            self.on_message = on_message
            self.on_status = on_status
            self.closed = False
            self.attempt = 0

        def _emit_snapshot(self, checksum: int):
            payload = {
                "channel": "book",
                "type": "snapshot",
                "data": [
                    {
                        "symbol": "BTC/USD",
                        "bids": [{"price": "100.0", "qty": "1.0"}],
                        "asks": [{"price": "101.0", "qty": "1.0"}],
                        "checksum": checksum,
                        "timestamp": "2025-12-15T12:00:00.000Z",
                    }
                ],
            }
            self.on_message(payload, 1000 + self.attempt)

        def _emit_update(self, checksum: int, qty: str = "2.0"):
            payload = {
                "channel": "book",
                "type": "update",
                "data": [
                    {
                        "symbol": "BTC/USD",
                        "bids": [{"price": "100.0", "qty": qty}],
                        "asks": [],
                        "checksum": checksum,
                        "timestamp": "2025-12-15T12:00:00.100Z",
                    }
                ],
            }
            self.on_message(payload, 2000 + self.attempt)

        def run(self):
            # First connect: snapshot + bad checksum update triggers resync.
            self.attempt += 1
            self.on_open()
            self._emit_snapshot(checksum=0)
            self._emit_update(checksum=123)

            # Second connect: snapshot + matching checksum update.
            self.attempt += 1
            self.on_open()
            self._emit_snapshot(checksum=0)
            self._emit_update(checksum=0)

        def run_forever(self):
            return self.run()

        def close(self):
            self.closed = True

        def disconnect(self):
            self.closed = True

    monkeypatch.setattr(recorder_mod, "BinanceWSStream", FakeStream)

    # Deterministic run_id
    monkeypatch.setattr(recorder_mod.time, "time", lambda: 1.0)

    recorder_mod.run_recorder()

    day = recorder_mod.compute_window(recorder_mod.window_now())[0].strftime("%Y%m%d")
    day_dir = tmp_path / "data" / "kraken" / "BTCUSD" / day
    events_path = day_dir / f"events_BTCUSD_{day}.csv.gz"
    assert events_path.exists()

    rows = list(csv.reader(gzip.open(events_path, "rt", encoding="utf-8", newline="")))
    types = [r[4] for r in rows[1:]]

    assert "snapshot_loaded" in types
    assert "resync_start" in types
    assert "resync_done" in types
