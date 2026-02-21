from __future__ import annotations

from mm_recorder.exchanges.kraken import _as_level_list


def test_as_level_list_ignores_extra_fields():
    levels = [["100", "1", "1710000000.0"], ["101", "2"]]
    out = _as_level_list(levels)
    assert out == [["100", "1"], ["101", "2"]]


def test_as_level_list_skips_short_rows():
    levels = [["100"], []]
    out = _as_level_list(levels)
    assert out == []
