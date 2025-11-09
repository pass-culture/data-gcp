from collections.abc import Sequence

import pandas as pd

from src.constants import GTL_ID_COLUMN


def normalize_gtl(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize GTL IDs: convert to string, strip, replace empty/NaN with None,
    pad 7-digit numbers to 8 chars, enforce hierarchical null rule."""
    df = df.copy()

    # Convert to string and strip whitespace
    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].astype(str).str.strip()

    # Replace empty or NaN-like strings with None
    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].replace(
        {"": None, "nan": None, "None": None, "<NA>": None}
    )

    def pad_and_validate(gtl_id):
        if not isinstance(gtl_id, str) or not gtl_id.isdigit():
            return None

        # Pad 7-digit numbers to 8
        if len(gtl_id) == 7:
            gtl_id = gtl_id.zfill(8)
        elif len(gtl_id) != 8:
            return None  # invalid length

        # Must not start with "00"
        if gtl_id.startswith("00"):
            return None

        # Hierarchy validation: once a "00" pair appears, must be followed by "00"
        pairs = [gtl_id[i : i + 2] for i in range(0, 8, 2)]
        found_null = False
        for pair in pairs:
            if found_null and pair != "00":
                return None
            if pair == "00":
                found_null = True

        return gtl_id

    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].apply(pad_and_validate)

    return df


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

    missing_values = ["", "nan", "None", "<NA>"]
    df = df.copy()
    for col in columns:
        if col in df.columns:
            # Convert to string and strip whitespace
            df[col] = df[col].astype(str).str.strip()
            # Replace empty/missing value representations with None
            if df[col].isin(missing_values).any():
                df[col] = df[col].replace(missing_values, None)
    return df


def detach_single_occuring_metadata(
    df: pd.DataFrame,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Set single occurence of Metadata to None so it doen't biases the RW.

    Args:
        df: The input dataframe.
        columns: List of column names to check.

    Returns:
        A new dataframe with cleaned Metadata columns.
    """
    df = df.copy()

    for col in columns:
        if col not in df.columns:
            continue

        # Count occurrences (faster with groupby for large data)
        value_counts = df[col].value_counts()

        # Find singleton values
        singleton_values = value_counts[value_counts == 1].index

        if len(singleton_values) > 0:
            df.loc[df[col].isin(singleton_values), col] = None

    return df


def remove_rows_with_no_metadata(
    df: pd.DataFrame, metadata_list: list | None = None
) -> pd.DataFrame:
    """Filter out rows where all specified feature_link columns are null.

    Args:
        df (pd.DataFrame): Input dataframe.
        metadata_list: List of column names to check for null values.

    Returns:
        pd.DataFrame: Filtered dataframe
        (rows with at least one non-null in features_link remain).
    """
    if not metadata_list:
        return df

    # Keep rows where at least one metadata column is not null
    mask = df[metadata_list].notna().any(axis=1)
    return df[mask]


def remove_rows_with_bad_gtls(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with invalid GTL IDs from the dataframe.

    Filters out rows where the GTL ID starts with "00" (considered invalid)
    and rows where the GTL ID is null/NaN.

    Args:
        df: The input dataframe containing GTL ID column.

    Returns:
        A new dataframe with invalid GTL rows removed.
    """
    return df.loc[~df[GTL_ID_COLUMN].astype(str).str.startswith("00")].loc[
        lambda df: df.gtl_id.notna()
    ]
