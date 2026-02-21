from __future__ import annotations

import pytest

from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import OrderBookSyncEngine


def test_adopt_snapshot_requires_last_update_id():
    lob = LocalOrderBook()
    lob.last_update_id = None
    engine = OrderBookSyncEngine()

    with pytest.raises(ValueError, match="last_update_id"):
        engine.adopt_snapshot(lob)
