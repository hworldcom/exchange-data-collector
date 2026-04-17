from datetime import datetime, timezone
from types import SimpleNamespace

import mm_recorder.logging_config as logging_config


def test_setup_logging_uses_utc_date(monkeypatch, tmp_path):
    seen = {}

    def fake_now(tz):
        seen["tz"] = tz
        return datetime(2026, 4, 17, 0, 30, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(logging_config, "datetime", SimpleNamespace(now=fake_now))

    log_path = logging_config.setup_logging(component="recorder", subdir="BTCUSDC", base_dir=tmp_path)

    assert seen["tz"] == timezone.utc
    assert log_path == tmp_path / "recorder" / "BTCUSDC" / "2026-04-17.log"


def test_setup_logging_can_use_recording_window_date(tmp_path):
    log_path = logging_config.setup_logging(
        component="recorder",
        subdir="BTCUSDC",
        base_dir=tmp_path,
        date_str="2026-04-18",
    )

    assert log_path == tmp_path / "recorder" / "BTCUSDC" / "2026-04-18.log"


def test_setup_logging_formats_records_in_utc(tmp_path):
    log_path = logging_config.setup_logging(component="recorder", subdir="BTCUSDC", base_dir=tmp_path)

    logger = logging_config.logging.getLogger("market_data.recorder.test")
    logger.info("timestamp check")

    contents = log_path.read_text(encoding="utf-8")

    assert "timestamp check" in contents
    assert "1970-" not in contents
    for handler in logging_config.logging.getLogger().handlers:
        assert getattr(handler.formatter, "converter", None) is logging_config.time.gmtime
