from collections.abc import Sequence

import pandas as pd

from src.constants import GTL_ID_COLUMN, ID_COLUMN


def preprocess_metadata_dataframe(
    df: pd.DataFrame,
    metadata_columns: list[str],
) -> pd.DataFrame:
    """Preprocess metadata dataframe by normalizing and cleaning specified columns.

    This function normalizes the specified metadata columns by converting values
    to strings, stripping whitespace, replacing empty/NaN values with None,
    detaching single-occurrence metadata, and removing rows with no metadata.

    Args:
        df: The input dataframe containing metadata.
        metadata_columns: List of column names to preprocess.

    Returns:
        A new dataframe with preprocessed metadata columns.
    """

    all_columns = [ID_COLUMN, *metadata_columns]
    df_processed = (
        df.pipe(normalize_dataframe, columns=all_columns)
        .pipe(normalize_gtl_id)
        .pipe(detach_single_occuring_metadata, columns=metadata_columns)
        .pipe(remove_rows_with_no_metadata, metadata_list=list(metadata_columns))
    )
    return df_processed


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


#########################################################################
############## GTL ID Normalization Helpers #############################
#########################################################################


def _pad_gtl_id(gtl_id: str | None) -> str | None:
    """Pad 7-digit GTL IDs to 8 digits with leading zero.

    Args:
        gtl_id: GTL ID string to pad, or None.

    Returns:
        Padded 8-digit GTL ID, or None if invalid/None.
    """
    if gtl_id is None:
        return None
    if not isinstance(gtl_id, str):
        return None
    if not gtl_id.isdigit():
        return None
    if len(gtl_id) == 7:
        return gtl_id.zfill(8)
    if len(gtl_id) == 8:
        return gtl_id
    return None


def _validate_gtl_hierarchy(gtl_id: str | None) -> str | None:
    """Validate GTL ID hierarchy rules.

    Removes IDs starting with '00' and enforces hierarchical null rule:
    once '00' appears in a pair, all following pairs must be '00'.

    Args:
        gtl_id: GTL ID string to validate, or None.

    Returns:
        Valid GTL ID, or None if invalid/None.
    """
    if gtl_id is None:
        return None
    if gtl_id.startswith("00"):
        return None
    # Hierarchy validation
    pairs = [gtl_id[i : i + 2] for i in range(0, 8, 2)]
    found_null = False
    for pair in pairs:
        if pair == "00":
            found_null = True
        elif found_null:
            # Non-zero pair after a null pair → invalid
            return None
    return gtl_id


def normalize_gtl_id(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize GTL IDs:
    - Convert to string, strip spaces
    - Replace NaN/empty with None
    - Pad 7-digit IDs with leading 0
    - Remove IDs starting with '00'
    - Enforce hierarchical null rule (once '00' appears, following pairs must be '00')

    Args:
        df: DataFrame with GTL_ID_COLUMN.

    Returns:
        DataFrame with normalized GTL IDs.
    """
    df = df.copy()

    # Step 1: Convert to string and strip whitespace
    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].astype(str).str.strip()

    # Step 2: Replace empty or NaN-like strings with None
    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].replace(
        {"": None, "nan": None, "None": None, "<NA>": None}
    )

    # Step 3: Pad 7-digit GTLs to 8 digits
    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].apply(_pad_gtl_id)

    # Step 4: Validate hierarchy rule and remove GTLs starting with '00'
    df[GTL_ID_COLUMN] = df[GTL_ID_COLUMN].apply(_validate_gtl_hierarchy)

    return df
