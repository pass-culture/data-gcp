"""Monthly aggregation helpers shared by training and reporting.

Centralises the "complete month" rule so the metrics logged during training and
the metrics recomputed in the report stay consistent.
"""

import pandas as pd

# A calendar month is considered complete (and therefore usable for ratio metrics
# such as MAPE) only if it holds at least this many observed periods. Partial
# boundary months of a backtest window have near-zero actual sums that make MAPE
# explode, so they are excluded.
MIN_DAYS_FULL_MONTH = 28
MIN_WEEKS_FULL_MONTH = 4


def min_periods_for_freq(freq: str) -> int:
    """Return the minimum number of periods a complete month must contain."""
    return MIN_WEEKS_FULL_MONTH if str(freq).upper().startswith("W") else MIN_DAYS_FULL_MONTH


def aggregate_to_complete_months(
    df: pd.DataFrame,
    freq: str,
    value_cols: list[str],
    date_col: str = "ds",
    positive_col: str | None = None,
) -> pd.DataFrame:
    """Aggregate a frequency-level series to monthly sums, keeping only complete months.

    Args:
        df: Frequency-level data containing ``date_col`` and ``value_cols``.
        freq: Series frequency (``"D"`` for daily, ``"W-*"`` for weekly).
        value_cols: Columns to sum per month.
        date_col: Name of the date column.
        positive_col: Optional column; rows where it is ``<= 0`` are dropped before
            aggregation. Used to discard missing/zero actual days that would both
            understate the monthly total and shrink the completeness count.

    Returns:
        DataFrame with a ``month`` (Timestamp) column plus the summed ``value_cols``.
        If every month is partial, all months are kept as a fallback.
    """
    out = df.copy()
    if positive_col is not None and positive_col in out.columns:
        out = out[out[positive_col] > 0]
    out["month"] = pd.to_datetime(out[date_col]).dt.to_period("M").dt.to_timestamp()
    grouped = out.groupby("month")
    monthly = grouped.agg({col: "sum" for col in value_cols if col in out.columns})
    monthly["n_periods"] = grouped.size()
    monthly = monthly.reset_index()

    complete = monthly[monthly["n_periods"] >= min_periods_for_freq(freq)]
    if not complete.empty:
        monthly = complete
    return monthly.drop(columns="n_periods").reset_index(drop=True)
