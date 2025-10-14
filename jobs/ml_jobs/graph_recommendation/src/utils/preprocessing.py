from collections.abc import Sequence

import pandas as pd

from src.constants import DEFAULT_METADATA_COLUMNS


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


def filter_out_isolated_items(
    df: pd.DataFrame, features_link: list | None = DEFAULT_METADATA_COLUMNS
) -> pd.DataFrame:
    """Filter out rows where all specified feature_link columns are null.

    Args:
        df (pd.DataFrame): Input dataframe.
        features_link (list): List of column names to check for isolation.

    Returns:
        pd.DataFrame: Filtered dataframe
        (rows with at least one non-null in features_link remain).
    """
    if not features_link:
        return df

    features_link = list(features_link)

    df = df.copy()
    # Keep only rows where at least one of the feature_link columns is not null
    mask = df[features_link].isna().all(axis=1)
    return df[~mask]
