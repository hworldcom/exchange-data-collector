"""Schema/versioning helpers for recorded market data."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

SCHEMA_VERSION = 4


def write_schema(path: Path, files: Mapping[str, Any]) -> None:
    """Write a `schema.json` file.

    The content is intentionally small and stable so it can be consumed by
    notebooks and unit tests.
    """
    schema = {
        "schema_version": SCHEMA_VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "files": dict(files),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, indent=2, sort_keys=True))
