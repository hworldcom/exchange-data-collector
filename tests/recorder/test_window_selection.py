from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from mm_recorder.metadata import PriceTickInfo
import mm_recorder.recorder as rec


class _NoopStream:
    def __init__(self, **_kwargs):
        self.closed = False

    def run(self):
        return None

    def close(self):
        self.closed = True


def test_prestart_grace_selects_upcoming_midnight_window(monkeypatch):
    monkeypatch.setenv("WINDOW_TZ", "Europe/Berlin")
    monkeypatch.setenv("WINDOW_START_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_DAY_OFFSET", "1")
    monkeypatch.setenv("WINDOW_PRESTART_GRACE_SEC", "120")

    now = datetime(2026, 4, 14, 23, 59, 0, tzinfo=ZoneInfo("Europe/Berlin"))

    start, end = rec.select_startup_window(now)

    assert start == datetime(2026, 4, 15, 0, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    assert end == datetime(2026, 4, 16, 0, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))


def test_window_now_defaults_to_utc(monkeypatch):
    monkeypatch.delenv("WINDOW_TZ", raising=False)

    class _FakeDatetime:
        seen_tz = None

        @staticmethod
        def now(tz):
            _FakeDatetime.seen_tz = tz
            return datetime(2026, 4, 17, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(rec, "datetime", _FakeDatetime)

    now = rec.window_now()

    assert getattr(_FakeDatetime.seen_tz, "key", None) == "UTC"
    assert now.tzinfo is _FakeDatetime.seen_tz


def test_default_window_is_utc_midnight_to_midnight(monkeypatch):
    monkeypatch.delenv("WINDOW_TZ", raising=False)
    monkeypatch.delenv("WINDOW_START_HHMM", raising=False)
    monkeypatch.delenv("WINDOW_END_HHMM", raising=False)
    monkeypatch.delenv("WINDOW_END_DAY_OFFSET", raising=False)
    monkeypatch.delenv("WINDOW_PRESTART_GRACE_SEC", raising=False)

    now = datetime(2026, 4, 17, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

    start, end = rec.select_startup_window(now)

    assert start == datetime(2026, 4, 17, 0, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert end == datetime(2026, 4, 18, 0, 0, 0, tzinfo=ZoneInfo("UTC"))


def test_without_prestart_grace_uses_active_window(monkeypatch):
    monkeypatch.setenv("WINDOW_TZ", "Europe/Berlin")
    monkeypatch.setenv("WINDOW_START_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_DAY_OFFSET", "1")
    monkeypatch.setenv("WINDOW_PRESTART_GRACE_SEC", "0")

    now = datetime(2026, 4, 14, 23, 59, 0, tzinfo=ZoneInfo("Europe/Berlin"))

    start, end = rec.select_startup_window(now)

    assert start == datetime(2026, 4, 14, 0, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    assert end == datetime(2026, 4, 15, 0, 0, 0, tzinfo=ZoneInfo("Europe/Berlin"))


def test_prestart_grace_rejects_negative_value(monkeypatch):
    monkeypatch.setenv("WINDOW_PRESTART_GRACE_SEC", "-1")

    now = datetime(2026, 4, 14, 23, 59, 0, tzinfo=ZoneInfo("Europe/Berlin"))

    with pytest.raises(RuntimeError, match="WINDOW_PRESTART_GRACE_SEC"):
        rec.select_startup_window(now)


def test_recorder_sleeps_until_upcoming_window(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("EXCHANGE", "binance")
    monkeypatch.setenv("SYMBOL", "BTCUSDT")
    monkeypatch.setenv("WINDOW_TZ", "Europe/Berlin")
    monkeypatch.setenv("WINDOW_START_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_HHMM", "00:00")
    monkeypatch.setenv("WINDOW_END_DAY_OFFSET", "1")
    monkeypatch.setenv("WINDOW_PRESTART_GRACE_SEC", "120")

    now = datetime(2026, 4, 14, 23, 59, 0, tzinfo=ZoneInfo("Europe/Berlin"))
    sleeps = []
    logging_kwargs = {}

    monkeypatch.setattr(rec, "window_now", lambda: now)
    monkeypatch.setattr(rec.time, "sleep", sleeps.append)
    monkeypatch.setattr(rec.time, "time", lambda: 1700000000.0)

    def _setup_logging(*args, **kwargs):
        logging_kwargs.update(kwargs)
        return Path("logs/test.log")

    monkeypatch.setattr(rec, "setup_logging", _setup_logging)
    monkeypatch.setattr(rec, "BinanceWSStream", _NoopStream)
    monkeypatch.setattr(
        rec,
        "resolve_price_tick_size",
        lambda *args, **kwargs: PriceTickInfo(
            exchange="binance",
            symbol="BTCUSDT",
            tick_size=Decimal("0.01"),
            source="test",
            base_asset="BTC",
            quote_asset="USDT",
            asset_source="test",
        ),
    )

    rec.run_recorder()

    assert sleeps == [60.0]
    assert logging_kwargs["date_str"] == "2026-04-15"
    assert (tmp_path / "data" / "binance" / "BTCUSDT" / "20260415").exists()
