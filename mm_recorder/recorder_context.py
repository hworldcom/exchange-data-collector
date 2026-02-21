from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from mm_recorder.buffered_writer import BufferedCSVWriter, BufferedTextWriter
from mm_recorder.live_writer import LiveNdjsonWriter
from mm_recorder.recorder_types import RecorderState


@dataclass
class RecorderContext:
    adapter: object
    exchange: str
    symbol: str
    symbol_fs: str
    run_id: int
    day_dir: Path
    snapshots_dir: Path
    diffs_dir: Path
    trades_dir: Path
    window_end: datetime
    ws_url: str
    sub_depth: int
    log: logging.Logger
    engine: object
    state: RecorderState
    rest_client: object | None
    record_rest_snapshot_fn: object
    ob_writer: BufferedCSVWriter
    tr_writer: BufferedCSVWriter
    gap_f: object
    ev_f: object
    gap_w: csv.writer
    ev_w: csv.writer
    diff_writer: BufferedTextWriter | None
    tr_raw_writer: BufferedTextWriter
    live_diff_writer: LiveNdjsonWriter | None
    live_trade_writer: LiveNdjsonWriter | None
