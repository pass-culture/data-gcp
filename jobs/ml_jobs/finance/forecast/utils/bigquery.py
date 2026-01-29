"""BigQuery utilities for loading and saving data.

This module provides helper functions for interacting with Google BigQuery,
including loading tables and executing queries.
"""

import pandas as pd
from google.cloud import bigquery

from forecast.utils.constants import FINANCE_DATASET, GCP_PROJECT_ID


def get_client() -> bigquery.Client:
    """Get a BigQuery client instance.

    Returns:
        bigquery.Client: Authenticated BigQuery client.
    """
    return bigquery.Client()


def load_table(table_name: str) -> pd.DataFrame:
    """Load a table from BigQuery.

    Args:
        table_name: Name of the table to load.

    Returns:
        DataFrame containing the table data.

    Raises:
        ValueError: If table_name is empty or invalid.
    """
    if not table_name:
        raise ValueError("table_name cannot be empty")

    client = get_client()
    query = f"SELECT * FROM `{GCP_PROJECT_ID}.{FINANCE_DATASET}.{table_name}`"
    return client.query(query).to_dataframe()


def load_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return results as DataFrame.

    Args:
        query: SQL query string to execute.

    Returns:
        DataFrame containing query results.
    """
    client = get_client()
    return client.query(query).to_dataframe()


def save_df_as_bigquery_table(df: pd.DataFrame, table_name: str) -> None:
    """Save a DataFrame as a BigQuery table.

    Args:
        df: DataFrame to save.
        table_name: Name of the destination table.

    Raises:
        ValueError: If table_name is empty or df is empty.
    """
    if not table_name:
        raise ValueError("table_name cannot be empty")
    if df.empty:
        raise ValueError("DataFrame is empty, cannot save to BigQuery")

    client = get_client()
    table_id = f"{GCP_PROJECT_ID}.{FINANCE_DATASET}.{table_name}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
