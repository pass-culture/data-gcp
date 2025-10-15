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


def remove_rows_with_no_metadata(
    df: pd.DataFrame, metadata_list: list | None = None
) -> pd.DataFrame:
    """Filter out rows where all specified feature_link columns are null.

    Args:
        df (pd.DataFrame): Input dataframe.
        features_link (list): List of column names to check for isolation.

    Returns:
        pd.DataFrame: Filtered dataframe
        (rows with at least one non-null in features_link remain).
    """
    if not metadata_list:
        return df

    df = df.copy()
    # Keep only rows where at least one of the feature_link columns is not null
    return df.loc[lambda _df: ~_df[metadata_list].isna().all(axis=1)]
