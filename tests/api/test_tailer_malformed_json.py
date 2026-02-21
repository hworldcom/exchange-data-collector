from __future__ import annotations

import json
from pathlib import Path

from mm_api.tailer import TailState, tail_text_ndjson


def test_tail_text_ndjson_skips_bad_lines(tmp_path: Path):
    path = tmp_path / "test.ndjson"
    lines = [
        json.dumps({"ok": 1}),
        "{bad json}",
        json.dumps({"ok": 2}),
    ]
    path.write_text("\n".join(lines), encoding="utf-8")

    state = TailState()
    payloads = tail_text_ndjson(path, state)
    assert [p["ok"] for p in payloads] == [1, 2]
