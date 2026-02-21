"""Pytest configuration for mm-recorder."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CORE = ROOT / "mm_core"
if CORE.exists() and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep tests offline by default; metadata fetch can be enabled per-test if needed.
os.environ.setdefault("MM_METADATA_FETCH", "0")
os.environ.setdefault("MM_PRICE_TICK_SIZE", "0.01")
