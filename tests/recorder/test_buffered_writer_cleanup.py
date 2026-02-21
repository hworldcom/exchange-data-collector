from __future__ import annotations

import pytest

import mm_recorder.buffered_writer as bw


class DummyFile:
    def __init__(self) -> None:
        self.closed = False

    def write(self, _data: str) -> int:
        return 0

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


def test_buffered_csv_writer_closes_on_open_failure(tmp_path, monkeypatch):
    dummy = DummyFile()
    monkeypatch.setattr(bw, "_is_empty_text_file", lambda _path: (_ for _ in ()).throw(RuntimeError("boom")))

    path = tmp_path / "out.csv"
    path.write_text("")
    writer = bw.BufferedCSVWriter(path, header=["a"], opener=lambda _p: dummy)

    with pytest.raises(RuntimeError, match="boom"):
        writer.ensure_file()

    assert dummy.closed is True
    assert writer._file is None
    assert writer._writer is None
