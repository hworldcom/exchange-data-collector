from __future__ import annotations

import pytest

import mm_recorder.buffered_writer as bw


def test_buffered_text_writer_open_failure_keeps_file_none(tmp_path):
    def _bad_opener(_path):
        raise RuntimeError("boom")

    writer = bw.BufferedTextWriter(tmp_path / "out.txt", opener=_bad_opener)

    with pytest.raises(RuntimeError, match="boom"):
        writer._ensure_open()

    assert writer._file is None
