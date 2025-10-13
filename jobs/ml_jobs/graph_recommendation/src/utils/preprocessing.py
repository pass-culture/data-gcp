from collections.abc import Sequence

import pandas as pd


def normalize_dataframe(
    df: pd.DataFrame,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Normalize specified columns in dataframe using vectorized operations.

    Converts all values to strings, strips whitespace, and replaces empty/NaN
    values with None. This is more efficient than row-by-row normalization.

    Args:
        df: The input dataframe.
        columns: List of column names to normalize.

    Returns:
        A new dataframe with normalized columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            # Convert to string and strip whitespace
            df[col] = df[col].astype(str).str.strip()
            # Replace empty/missing value representations with None
            df[col] = df[col].replace(["", "nan", "None", "<NA>"], None)
    return df
