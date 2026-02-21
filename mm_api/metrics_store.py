from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple


@dataclass
class CloseSeries:
    timestamps: Deque[int]
    closes: Deque[float]

    def __init__(self) -> None:
        self.timestamps = deque()
        self.closes = deque()

    def append(self, ts_ms: int, close: float) -> None:
        if self.timestamps and self.timestamps[-1] == ts_ms:
            self.closes[-1] = close
            return
        self.timestamps.append(ts_ms)
        self.closes.append(close)

    def trim_before(self, min_ts_ms: int) -> None:
        while self.timestamps and self.timestamps[0] < min_ts_ms:
            self.timestamps.popleft()
            self.closes.popleft()

    def as_list(self) -> List[Tuple[int, float]]:
        return list(zip(self.timestamps, self.closes))


def compute_returns(series: CloseSeries) -> List[float]:
    returns: List[float] = []
    prev = None
    for close in series.closes:
        if prev is None:
            prev = close
            continue
        if prev > 0:
            returns.append((close / prev) - 1.0)
        prev = close
    return returns


def compute_volatility(returns: List[float]) -> Optional[float]:
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return var ** 0.5


def compute_correlation(a: List[float], b: List[float]) -> Optional[float]:
    n = min(len(a), len(b))
    if n < 2:
        return None
    a = a[-n:]
    b = b[-n:]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    cov = sum((ai - mean_a) * (bi - mean_b) for ai, bi in zip(a, b))
    var_a = sum((ai - mean_a) ** 2 for ai in a)
    var_b = sum((bi - mean_b) ** 2 for bi in b)
    if var_a == 0 or var_b == 0:
        return None
    return cov / (var_a ** 0.5 * var_b ** 0.5)
