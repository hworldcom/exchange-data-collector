from __future__ import annotations

from mm_api.metrics_store import CloseSeries, compute_correlation, compute_returns, compute_volatility


def test_close_series_append_and_trim() -> None:
    series = CloseSeries()
    series.append(1000, 10.0)
    series.append(2000, 11.0)
    series.append(2000, 12.0)
    assert list(series.timestamps) == [1000, 2000]
    assert list(series.closes) == [10.0, 12.0]
    series.trim_before(2000)
    assert list(series.timestamps) == [2000]


def test_compute_returns_and_volatility() -> None:
    series = CloseSeries()
    series.append(0, 10.0)
    series.append(1, 11.0)
    series.append(2, 12.1)
    returns = compute_returns(series)
    assert len(returns) == 2
    vol = compute_volatility(returns)
    assert vol is not None
    assert vol > 0


def test_compute_correlation() -> None:
    a = [0.1, 0.2, 0.3]
    b = [0.1, 0.2, 0.3]
    corr = compute_correlation(a, b)
    assert corr is not None
    assert abs(corr - 1.0) < 1e-9
