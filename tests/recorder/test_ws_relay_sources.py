from __future__ import annotations

from pathlib import Path

from mm_api.sources import resolve_latest_paths


def test_resolve_latest_paths_prefers_live(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    day_dir = tmp_path / "data" / "binance" / "BTCUSDT" / "20260205"
    live_dir = day_dir / "live"
    trades_dir = day_dir / "trades"
    diffs_dir = day_dir / "diffs"
    snapshots_dir = day_dir / "snapshots"
    day_dir.mkdir(parents=True)
    live_dir.mkdir()
    trades_dir.mkdir()
    diffs_dir.mkdir()
    snapshots_dir.mkdir()

    (live_dir / "live_depth_diffs.ndjson").write_text('{"x":1}\n', encoding="utf-8")
    (live_dir / "live_trades.ndjson").write_text('{"y":2}\n', encoding="utf-8")
    (diffs_dir / "depth_diffs_BTCUSDT_20260205.ndjson.gz").write_text("", encoding="utf-8")
    (trades_dir / "trades_ws_raw_BTCUSDT_20260205.ndjson.gz").write_text("", encoding="utf-8")
    (snapshots_dir / "snapshot_1_initial.json").write_text("{}", encoding="utf-8")

    paths = resolve_latest_paths("binance", "BTCUSDT")
    assert paths["live_diffs"].name == "live_depth_diffs.ndjson"
    assert paths["live_trades"].name == "live_trades.ndjson"
    assert paths["snapshot"].name == "snapshot_1_initial.json"

