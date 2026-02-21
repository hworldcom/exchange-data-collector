# mm/market_data/recorder.py

import os
import csv
import time
import gzip
import logging
from pathlib import Path
from contextlib import ExitStack
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

"""Market data recorder.

Note: The project supports running unit tests in minimal environments where
`python-binance` may not be installed. We therefore import it lazily.
"""

from mm_recorder.logging_config import setup_logging
from mm_recorder.ws_stream import BinanceWSStream
from mm_recorder.snapshot import (
    make_rest_client,
    record_rest_snapshot,
    SNAPSHOT_TIMEOUT_S,
    SNAPSHOT_RETRY_MAX,
    SNAPSHOT_RETRY_BACKOFF_S,
    SNAPSHOT_RETRY_BACKOFF_MAX_S,
)
from mm_recorder.buffered_writer import BufferedCSVWriter, BufferedTextWriter, _is_empty_text_file
from mm_recorder.live_writer import LiveNdjsonWriter
from mm_recorder.exchanges import get_adapter
from mm_recorder.metadata import (
    resolve_price_tick_size,
    BINANCE_REST_BASE_URL,
    KRAKEN_REST_BASE_URL,
    BITFINEX_REST_BASE_URL,
    METADATA_TIMEOUT_S,
    METADATA_RETRY_MAX,
    METADATA_RETRY_BACKOFF_S,
    METADATA_RETRY_BACKOFF_MAX_S,
)
from mm_core.schema import write_schema, SCHEMA_VERSION
from mm_core.symbols import symbol_fs as symbol_fs_fn
from mm_core.local_orderbook import set_default_tick_size
from mm_recorder.recorder_callbacks import RecorderCallbacks
from mm_recorder.recorder_context import RecorderContext
from mm_recorder.recorder_settings import (
    DECIMALS,
    DEPTH_LEVELS,
    HEARTBEAT_SEC,
    MAX_BUFFER_WARN,
    SNAPSHOT_LIMIT,
    ORDERBOOK_BUFFER_ROWS,
    TRADES_BUFFER_ROWS,
    BUFFER_FLUSH_INTERVAL_SEC,
    WS_PING_INTERVAL_S,
    WS_PING_TIMEOUT_S,
    WS_RECONNECT_BACKOFF_S,
    WS_RECONNECT_BACKOFF_MAX_S,
    WS_MAX_SESSION_S,
    WS_OPEN_TIMEOUT_S,
    WS_NO_DATA_WARN_S,
    INSECURE_TLS,
    STORE_DEPTH_DIFFS,
    LIVE_STREAM_ENABLED,
    LIVE_STREAM_ROTATE_S,
    LIVE_STREAM_RETENTION_S,
    SYNC_WARN_AFTER_SEC,
)
from mm_recorder.recorder_types import RecorderPhase, RecorderState

ORIGINAL_RECORD_REST_SNAPSHOT = record_rest_snapshot

 

def window_now():
    """Current wall-clock time in the configured recording timezone.

    We intentionally read environment variables at call time so unit tests
    (and production launch scripts) can override the window parameters
    without requiring a module reload.
    """
    tz = os.getenv("WINDOW_TZ", "Europe/Berlin")
    return datetime.now(ZoneInfo(tz))

def _parse_hhmm(value: str, label: str) -> tuple[int, int]:
    try:
        hour_str, minute_str = value.strip().split(":")
        hour = int(hour_str)
        minute = int(minute_str)
    except Exception as exc:
        raise RuntimeError(f"{label} must be in HH:MM format (got {value!r}).") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise RuntimeError(f"{label} must be a valid 24h time (got {value!r}).")
    return hour, minute


def compute_window(now: datetime) -> tuple[datetime, datetime]:
    # Read env at runtime (not import time) so tests and launchers can set
    # the recording window via environment variables.
    start_hhmm = os.getenv("WINDOW_START_HHMM", "00:00")
    end_hhmm = os.getenv("WINDOW_END_HHMM", "00:15")
    end_day_offset = int(os.getenv("WINDOW_END_DAY_OFFSET", "1"))

    start_h, start_m = _parse_hhmm(start_hhmm, "WINDOW_START_HHMM")
    end_h, end_m = _parse_hhmm(end_hhmm, "WINDOW_END_HHMM")

    start = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0) + timedelta(days=end_day_offset)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def run_recorder():
    exchange = os.getenv("EXCHANGE", "binance").strip().lower()
    adapter = get_adapter(exchange)
    symbol = adapter.normalize_symbol(os.getenv("SYMBOL", "").strip())
    symbol_fs = symbol_fs_fn(symbol)
    if not symbol:
        raise RuntimeError("SYMBOL environment variable is required (e.g. SYMBOL=BTCUSDT).")

    rest_client = make_rest_client(exchange)
    if adapter.sync_mode == "sequence" and rest_client is None:
        raise RuntimeError(f"No REST snapshot client configured for exchange={exchange}")

    now = window_now()
    window_start, window_end = compute_window(now)
    if now < window_start:
        prev_start = window_start - timedelta(days=1)
        prev_end = window_end - timedelta(days=1)
        if now <= prev_end:
            window_start = prev_start
            window_end = prev_end

    # Per-day folder (window start date)
    day_str = window_start.strftime("%Y%m%d")
    symbol_dir = Path("data") / exchange / symbol_fs
    day_dir = symbol_dir / day_str
    day_dir.mkdir(parents=True, exist_ok=True)

    snapshots_dir = day_dir / "snapshots"
    diffs_dir = day_dir / "diffs"
    trades_dir = day_dir / "trades"
    if STORE_DEPTH_DIFFS:
        diffs_dir.mkdir(parents=True, exist_ok=True)
    trades_dir.mkdir(parents=True, exist_ok=True)

    log_subdir = f"{exchange}/{symbol_fs}"
    log_path = setup_logging("INFO", component="recorder", subdir=log_subdir)
    log = logging.getLogger("market_data.recorder")
    log.info("Recorder logging to %s", log_path)

    def _safe_close(obj, label: str) -> None:
        if obj is None:
            return
        try:
            obj.close()
        except Exception:
            log.exception("Failed to close %s during setup cleanup", label)

    tick_info = resolve_price_tick_size(exchange, symbol, log=log)
    set_default_tick_size(tick_info.tick_size)
    price_precision = None
    if exchange == "bitfinex" and isinstance(tick_info.raw, dict):
        price_precision = tick_info.raw.get("price_precision")
    if exchange == "bitfinex" and price_precision is not None:
        log.info(
            "Bitfinex price precision=%s (significant digits); tick policy=dynamic (source=%s)",
            price_precision,
            tick_info.source,
        )
    else:
        log.info("Price tick size=%s (source=%s)", tick_info.tick_size, tick_info.source)

    log.info(
        "Recorder config exchange=%s symbol=%s symbol_fs=%s tick_size=%s tick_source=%s window=%s–%s tz=%s depth_levels=%s store_depth_diffs=%s",
        exchange,
        symbol,
        symbol_fs,
        (tick_info.tick_size if exchange != "bitfinex" else "dynamic"),
        tick_info.source,
        window_start.isoformat(),
        window_end.isoformat(),
        os.getenv("WINDOW_TZ", "Europe/Berlin"),
        DEPTH_LEVELS,
        STORE_DEPTH_DIFFS,
    )
    log.info(
        "WS config ping_interval_s=%s ping_timeout_s=%s reconnect_backoff_s=%.2f reconnect_backoff_max_s=%.2f max_session_s=%.0f",
        WS_PING_INTERVAL_S,
        WS_PING_TIMEOUT_S,
        WS_RECONNECT_BACKOFF_S,
        WS_RECONNECT_BACKOFF_MAX_S,
        WS_MAX_SESSION_S,
    )
    log.info("WS connect timeout open_timeout_s=%.1f", WS_OPEN_TIMEOUT_S)

    # Recording window in configured timezone.
    start = window_start
    end = window_end

    # Current wall-clock time. Used to decide whether to sleep until
    # the recording window starts.
    if now > end:
        log.info("Now is past end of window (%s). Exiting.", end.isoformat())
        return

    if now < start:
        sleep_s = (start - now).total_seconds()
        log.info("Before start window. Sleeping %.1fs until %s.", sleep_s, start.isoformat())
        time.sleep(max(0.0, sleep_s))

    # Run-scoped ids for audit and file naming
    run_id = int(time.time() * 1000)

    sub_depth = adapter.normalize_depth(DEPTH_LEVELS)

    # Outputs
    ob_path = day_dir / f"orderbook_ws_depth_{symbol_fs}_{day_str}.csv.gz"
    tr_path = day_dir / f"trades_ws_{symbol_fs}_{day_str}.csv.gz"
    gap_path = day_dir / f"gaps_{symbol_fs}_{day_str}.csv.gz"
    ev_path = day_dir / f"events_{symbol_fs}_{day_str}.csv.gz"

    def open_csv_append(path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        if path.suffix == ".gz":
            f = gzip.open(path, "at", encoding="utf-8", newline="")
        else:
            f = path.open("a", newline="")
        is_new = (not existed) or _is_empty_text_file(path)
        return f, is_new

    ob_header = (
        ["event_time_ms", "recv_time_ms", "recv_seq", "run_id", "epoch_id"]
        + sum(
            [
                [f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]
                for i in range(1, DEPTH_LEVELS + 1)
            ],
            [],
        )
    )
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




    # Write per-day schema metadata for controlled format evolution.
    # This file is overwritten on each recorder start to reflect current schema.
    schema_path = day_dir / "schema.json"
    files_schema = {
            "orderbook_ws_depth_csv": {
            "path": str(ob_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": ob_header,
        },
        "trades_ws_csv": {
            "path": str(tr_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": tr_header,
        },
        "gaps_csv": {
            "path": str(gap_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": ["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"],
        },
        "events_csv": {
            "path": str(ev_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": ["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"],
        },
        "snapshots_raw_json": {
            "path": "snapshots/snapshot_<event_id>_<tag>.json",
            "format": "json",
            "notes": "Raw exchange snapshot payload (REST for Binance, WS for checksum exchanges).",
        },
        "trades_ws_raw_ndjson_gz": {
            "path": f"trades/trades_ws_raw_{symbol_fs}_{day_str}.ndjson.gz",
            "format": "ndjson.gz",
            "fields": ["recv_ms", "recv_seq", "event_time_ms", "trade_id", "exchange", "symbol", "raw"],
        },
    }
    if STORE_DEPTH_DIFFS:
        diff_fields = ["recv_ms", "recv_seq", "E", "U", "u", "b", "a"]
        if adapter.sync_mode == "checksum":
            diff_fields.append("checksum")
        diff_fields.extend(["exchange", "symbol", "raw"])
        files_schema["depth_diffs_ndjson_gz"] = {
            "path": f"diffs/depth_diffs_{symbol_fs}_{day_str}.ndjson.gz",
            "format": "ndjson.gz",
            "fields": diff_fields,
            "depth": sub_depth,
        }
    write_schema(schema_path, files_schema)

    setup_stack = ExitStack()

    ob_writer = BufferedCSVWriter(
        ob_path,
        header=ob_header,
        flush_rows=ORDERBOOK_BUFFER_ROWS,
        flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
    )
    tr_writer = BufferedCSVWriter(
        tr_path,
        header=tr_header,
        flush_rows=TRADES_BUFFER_ROWS,
        flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
    )
    setup_stack.callback(_safe_close, ob_writer, "orderbook_writer")
    setup_stack.callback(_safe_close, tr_writer, "trades_writer")

    ob_writer.ensure_file()
    tr_writer.ensure_file()

    gap_f, gap_new = open_csv_append(gap_path)
    ev_f, ev_new = open_csv_append(ev_path)
    setup_stack.callback(_safe_close, gap_f, f"file {getattr(gap_f, 'name', 'unknown')}")
    setup_stack.callback(_safe_close, ev_f, f"file {getattr(ev_f, 'name', 'unknown')}")

    gap_w = csv.writer(gap_f)
    ev_w = csv.writer(ev_f)

    if gap_new:
        gap_w.writerow(["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"])
        gap_f.flush()

    if ev_new:
        ev_w.writerow(["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"])
        ev_f.flush()
    diff_writer: BufferedTextWriter | None = None
    if STORE_DEPTH_DIFFS:
        diff_path = diffs_dir / f"depth_diffs_{symbol_fs}_{day_str}.ndjson.gz"
        # Buffer gzip writes to avoid per-message flush costs.
        diff_writer = BufferedTextWriter(
            diff_path,
            flush_lines=5000,
            flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
            opener=lambda p: gzip.open(p, "at", encoding="utf-8"),
        )
        setup_stack.callback(_safe_close, diff_writer, "diff_writer")
    tr_raw_writer = BufferedTextWriter(
        trades_dir / f"trades_ws_raw_{symbol_fs}_{day_str}.ndjson.gz",
        flush_lines=5000,
        flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
        opener=lambda p: gzip.open(p, "at", encoding="utf-8"),
    )
    setup_stack.callback(_safe_close, tr_raw_writer, "trades_raw_writer")
    live_diff_writer: LiveNdjsonWriter | None = None
    live_trade_writer: LiveNdjsonWriter | None = None
    if LIVE_STREAM_ENABLED:
        live_dir = day_dir / "live"
        live_diff_writer = LiveNdjsonWriter(
            live_dir / "live_depth_diffs.ndjson",
            rotate_interval_s=LIVE_STREAM_ROTATE_S,
            retention_s=LIVE_STREAM_RETENTION_S,
        )
        live_trade_writer = LiveNdjsonWriter(
            live_dir / "live_trades.ndjson",
            rotate_interval_s=LIVE_STREAM_ROTATE_S,
            retention_s=LIVE_STREAM_RETENTION_S,
        )
        setup_stack.callback(_safe_close, live_diff_writer, "live_writer")
        setup_stack.callback(_safe_close, live_trade_writer, "live_writer")

    log.info("Day dir:         %s", day_dir)
    log.info("Orderbook out:   %s", ob_path)
    log.info("Trades out:      %s", tr_path)
    log.info("Gaps out:        %s", gap_path)
    log.info("Events out:      %s", ev_path)
    if STORE_DEPTH_DIFFS:
        log.info("Diffs out:       %s", diff_path)
    if LIVE_STREAM_ENABLED:
        log.info("Live diffs out:  %s", live_dir / "live_depth_diffs.ndjson")
        log.info("Live trades out: %s", live_dir / "live_trades.ndjson")

    ws_url = adapter.ws_url(symbol)

    engine_kwargs: dict = {}
    if exchange == "bitfinex":
        if isinstance(tick_info.raw, dict) and tick_info.raw.get("price_precision") is not None:
            try:
                engine_kwargs["price_precision"] = int(tick_info.raw.get("price_precision"))
            except Exception:
                log.warning("Invalid Bitfinex price_precision in metadata: %r", tick_info.raw.get("price_precision"))
    engine = adapter.create_sync_engine(sub_depth, **engine_kwargs)
    engine_buffer_max = getattr(engine, "max_buffer_size", None)

    log.info(
        "Startup summary exchange=%s symbol=%s symbol_fs=%s ws_url=%s run_id=%s",
        exchange,
        symbol,
        symbol_fs,
        ws_url,
        run_id,
    )
    log.info(
        "Startup window start=%s end=%s tz=%s day_dir=%s",
        window_start.isoformat(),
        window_end.isoformat(),
        os.getenv("WINDOW_TZ", "Europe/Berlin"),
        day_dir,
    )
    log.info(
        "Startup metadata tick_size=%s source=%s metadata_fetch=%s metadata_strict=%s price_tick_override=%s",
        tick_info.tick_size,
        tick_info.source,
        os.getenv("MM_METADATA_FETCH", "1"),
        os.getenv("MM_METADATA_STRICT", "1"),
        ("set" if os.getenv("MM_PRICE_TICK_SIZE") else "unset"),
    )
    log.info(
        "Startup metadata endpoints binance=%s kraken=%s bitfinex=%s timeout_s=%.1f retries=%s backoff_s=%.2f backoff_max_s=%.2f",
        BINANCE_REST_BASE_URL,
        KRAKEN_REST_BASE_URL,
        BITFINEX_REST_BASE_URL,
        METADATA_TIMEOUT_S,
        METADATA_RETRY_MAX,
        METADATA_RETRY_BACKOFF_S,
        METADATA_RETRY_BACKOFF_MAX_S,
    )
    log.info(
        "Startup snapshot config limit=%s timeout_s=%.1f retries=%s backoff_s=%.2f backoff_max_s=%.2f",
        SNAPSHOT_LIMIT,
        SNAPSHOT_TIMEOUT_S,
        SNAPSHOT_RETRY_MAX,
        SNAPSHOT_RETRY_BACKOFF_S,
        SNAPSHOT_RETRY_BACKOFF_MAX_S,
    )
    log.info(
        "Startup buffers depth_levels=%s store_depth_diffs=%s live_stream=%s ob_flush_rows=%s tr_flush_rows=%s flush_interval_s=%.1f max_sync_buffer=%s",
        DEPTH_LEVELS,
        STORE_DEPTH_DIFFS,
        LIVE_STREAM_ENABLED,
        ORDERBOOK_BUFFER_ROWS,
        TRADES_BUFFER_ROWS,
        BUFFER_FLUSH_INTERVAL_SEC,
        engine_buffer_max,
    )

    state = RecorderState(
        event_id=int(time.time() * 1000),
        last_hb=time.time(),
        sync_t0=time.time(),
        last_sync_warn=time.time(),
    )

    ctx = RecorderContext(
        adapter=adapter,
        exchange=exchange,
        symbol=symbol,
        symbol_fs=symbol_fs,
        run_id=run_id,
        day_dir=day_dir,
        snapshots_dir=snapshots_dir,
        diffs_dir=diffs_dir,
        trades_dir=trades_dir,
        window_end=end,
        ws_url=ws_url,
        sub_depth=sub_depth,
        log=log,
        engine=engine,
        state=state,
        rest_client=rest_client,
        record_rest_snapshot_fn=record_rest_snapshot,
        ob_writer=ob_writer,
        tr_writer=tr_writer,
        gap_f=gap_f,
        ev_f=ev_f,
        gap_w=gap_w,
        ev_w=ev_w,
        diff_writer=diff_writer,
        tr_raw_writer=tr_raw_writer,
        live_diff_writer=live_diff_writer,
        live_trade_writer=live_trade_writer,
    )

    callbacks = None
    stream = None
    runtime_stack = None
    try:
        callbacks = RecorderCallbacks(ctx, window_now)

        # Backwards-compatible construction: tests may monkeypatch BinanceWSStream with a
        # simplified fake that does not accept newer parameters.
        try:
            stream = BinanceWSStream(
                ws_url=ws_url,
                on_depth=callbacks.on_depth,
                on_trade=callbacks.on_trade,
                on_open=callbacks.on_open,
                on_status=callbacks.on_status,
                on_message=(callbacks.on_message if adapter.uses_custom_ws_messages else None),
                insecure_tls=INSECURE_TLS,
                ping_interval_s=WS_PING_INTERVAL_S,
                ping_timeout_s=WS_PING_TIMEOUT_S,
                reconnect_backoff_s=WS_RECONNECT_BACKOFF_S,
                reconnect_backoff_max_s=WS_RECONNECT_BACKOFF_MAX_S,
                max_session_s=WS_MAX_SESSION_S,
                open_timeout_s=WS_OPEN_TIMEOUT_S,
                subscribe_messages=adapter.subscribe_messages(symbol, sub_depth),
            )
        except TypeError:
            stream = BinanceWSStream(
                ws_url=ws_url,
                on_depth=callbacks.on_depth,
                on_trade=callbacks.on_trade,
                on_open=callbacks.on_open,
                insecure_tls=INSECURE_TLS,
            )

        runtime_stack = setup_stack.pop_all()

        callbacks.attach_stream(stream)

        callbacks.emit_event("run_start", {"symbol": symbol, "symbol_fs": symbol_fs, "day": day_str})
        log.info("Connecting WS: %s", ws_url)

        run_fn = getattr(stream, "run", None) or getattr(stream, "run_forever", None)
        if run_fn is None:
            raise RuntimeError("BinanceWSStream has no run()/run_forever()")
        run_fn()
    finally:
        if callbacks is not None:
            try:
                callbacks.shutdown()
            except Exception:
                log.exception("Recorder shutdown raised")
        try:
            (runtime_stack or setup_stack).close()
        except Exception:
            log.exception("Recorder setup cleanup raised")


def main():
    # Ensure we always surface exceptions in logs (both file and stdout).
    # In cron/docker contexts stderr may be discarded, so we log the traceback.
    try:
        run_recorder()
    except Exception:
        logging.getLogger("market_data.recorder").exception("Recorder crashed")
        raise


if __name__ == "__main__":
    main()
