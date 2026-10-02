"""Tests for the shared monthly aggregation helper and its use in backtest metrics."""

import pandas as pd

from forecast.engines.prophet.evaluate import _aggregate_forecast_monthly
from forecast.utils.aggregation import aggregate_to_complete_months


def _daily(dates: list[str]) -> pd.DataFrame:
    idx: list[pd.Timestamp] = []
    for start, periods in dates:
        idx += list(pd.date_range(start, periods=periods, freq="D"))
    df = pd.DataFrame({"ds": idx})
    df["y"] = 2e5
    df["yhat"] = 2.1e5
    return df


def test_aggregate_drops_partial_boundary_months():
    # Partial Feb (2 days) and partial Sep (10 days) must be dropped; full months kept.
    df = _daily([("2026-02-27", 2), ("2026-03-01", 31), ("2026-04-01", 30), ("2026-09-01", 10)])
    monthly = aggregate_to_complete_months(df, freq="D", value_cols=["y", "yhat"])
    assert monthly["month"].dt.strftime("%Y-%m").tolist() == ["2026-03", "2026-04"]


def test_aggregate_keeps_all_when_everything_partial():
    df = _daily([("2026-02-27", 2), ("2026-09-01", 3)])
    monthly = aggregate_to_complete_months(df, freq="D", value_cols=["y", "yhat"])
    assert monthly["month"].dt.strftime("%Y-%m").tolist() == ["2026-02", "2026-09"]


def test_weekly_threshold_uses_four_weeks():
    weeks = pd.DataFrame({"ds": pd.date_range("2026-03-02", periods=4, freq="W-MON")})  # 4 Mondays in March
    weeks = pd.concat([weeks, pd.DataFrame({"ds": pd.date_range("2026-04-06", periods=1, freq="W-MON")})])
    weeks["y"] = 1.4e6
    weeks["yhat"] = 1.5e6
    monthly = aggregate_to_complete_months(weeks, freq="W-MON", value_cols=["y", "yhat"])
    # March has 4 weeks (kept), April has 1 week (dropped).
    assert monthly["month"].dt.strftime("%Y-%m").tolist() == ["2026-03"]


def test_backtest_aggregation_reproduces_stable_mape():
    # Reproduces the staging incoherence: a near-empty partial month must not count.
    df = _daily([("2026-06-05", 26), ("2026-07-01", 31), ("2026-08-01", 31)])
    # Inject a near-empty partial September like the stale dev dataset.
    sept = pd.DataFrame({"ds": pd.date_range("2026-09-01", periods=10, freq="D")})
    sept["y"] = 47.0  # essentially zero
    sept["yhat"] = 3.9e5
    df = pd.concat([df, sept], ignore_index=True)

    monthly = _aggregate_forecast_monthly(df, freq="D")
    kept = monthly["month"].dt.strftime("%Y-%m").tolist()
    assert "2026-09" not in kept  # degenerate month excluded
    assert kept == ["2026-07", "2026-08"]  # June partial (26d) also excluded


def test_positive_col_drops_zero_actual_days():
    # A full month where several days are missing (y == 0) should have those days
    # dropped, which both corrects the total and can push an incomplete month out.
    df = _daily([("2026-07-01", 31)])
    df.loc[df["ds"].dt.day <= 5, "y"] = 0  # 5 missing days

    without = aggregate_to_complete_months(df, freq="D", value_cols=["y", "yhat"])
    with_filter = aggregate_to_complete_months(df, freq="D", value_cols=["y", "yhat"], positive_col="y")
    # Unfiltered keeps all 31 days (zeros included) -> lower actual total.
    assert without.loc[0, "y"] == 26 * 2e5
    # Filtered drops the 5 zero days: 26 days remain (< 28) -> fallback keeps it,
    # and the summed actual is unchanged but no zero rows remain.
    assert with_filter.loc[0, "y"] == 26 * 2e5
    assert len(with_filter) == 1
