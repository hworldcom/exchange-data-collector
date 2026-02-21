from __future__ import annotations


def symbol_fs(symbol: str, *, upper: bool = False) -> str:
    """Normalize a symbol for filesystem paths by stripping separators/spaces.

    Use upper=True when callers require case-insensitive folder names.
    """
    cleaned = (
        symbol.replace("/", "")
        .replace("-", "")
        .replace(":", "")
        .replace(" ", "")
    )
    return cleaned.upper() if upper else cleaned
