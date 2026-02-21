from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RecorderPhase(str, Enum):
    CONNECTING = "connecting"
    SNAPSHOT = "snapshot"
    SYNCING = "syncing"
    SYNCED = "synced"
    RESYNCING = "resyncing"
    STOPPED = "stopped"


@dataclass
class RecorderState:
    recv_seq: int = 0
    event_id: int = 0
    epoch_id: int = 0
    resync_count: int = 0
    ws_open_count: int = 0
    window_end_emitted: bool = False
    last_hb: float = 0.0
    sync_t0: float = 0.0
    last_sync_warn: float = 0.0
    depth_msg_count: int = 0
    trade_msg_count: int = 0
    ob_rows_written: int = 0
    tr_rows_written: int = 0
    last_depth_event_ms: int | None = None
    last_trade_event_ms: int | None = None
    needs_snapshot: bool = False
    pending_snapshot_tag: str | None = None
    phase: RecorderPhase = RecorderPhase.CONNECTING
    last_ws_msg_time: float | None = None
    last_no_data_warn: float = 0.0
    first_data_emitted: bool = False
