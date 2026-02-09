"""BigQuery utilities for loading and saving data.

This module provides helper functions for interacting with Google BigQuery,
including loading tables and executing queries.
"""

import pandas as pd
from google.cloud import bigquery

from forecast.utils.constants import GCP_PROJECT_ID


def get_client() -> bigquery.Client:
    """Get a BigQuery client instance.

    Returns:
        bigquery.Client: Authenticated BigQuery client.
    """
    return bigquery.Client()


def load_table(dataset: str, table_name: str) -> pd.DataFrame:
    """Load a table from BigQuery.

    Args:
        dataset: BigQuery dataset containing the table.
        table_name: Name of the table to load.

    Returns:
        DataFrame containing the table data.

    Raises:
        ValueError: If table_name is empty or invalid.
    """
    if not table_name:
        raise ValueError("table_name cannot be empty")

    client = get_client()
    query = f"SELECT * FROM `{GCP_PROJECT_ID}.{dataset}.{table_name}`"
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


def save_forecast_gbq(
    df: pd.DataFrame,
    run_id: str,
    experiment_name: str,
    run_name: str,
    model_name: str,
    model_type: str,
    table_name: str = "monthly_forecasts",
    dataset: str = "ml_finance_stg",
    if_exists: str = "append",
) -> None:
    """Save a forecast DataFrame to BigQuery.

    Args:
        df: DataFrame containing the monthly forecast data.
        run_id: MLflow run ID.
        experiment_name: MLflow experiment name.
        run_name: MLflow run name.
        model_name: Name of the model configuration.
        model_type: Type of model (e.g. 'prophet').
        table_name: Target table name within the finance dataset.
        dataset: BigQuery dataset containing input data and forecast output.
        if_exists: Behavior if table exists ('fail', 'replace', 'append').
    """
    destination_table = f"{GCP_PROJECT_ID}.{dataset}.{table_name}"

    # Transform DataFrame to BigQuery schema
    bq_df = df.copy()

    # Rename columns to match schema if needed
    if "ds" in bq_df.columns:
        bq_df = bq_df.rename(columns={"ds": "forecast_date"})
    if "total_pricing" in bq_df.columns:
        bq_df = bq_df.rename(columns={"total_pricing": "prediction"})

    # Add metadata columns
    bq_df["run_id"] = run_id
    bq_df["experiment_name"] = experiment_name
    bq_df["run_name"] = run_name
    bq_df["model_name"] = model_name
    bq_df["model_type"] = model_type
    bq_df["execution_date"] = pd.Timestamp.now()

    # Define schema explicitly to ensure consistency
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("forecast_date", "DATE"),
            bigquery.SchemaField("prediction", "FLOAT"),
            bigquery.SchemaField("model_name", "STRING"),
            bigquery.SchemaField("model_type", "STRING"),
            bigquery.SchemaField("experiment_name", "STRING"),
            bigquery.SchemaField("run_name", "STRING"),
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("execution_date", "TIMESTAMP"),
        ],
        write_disposition="WRITE_APPEND" if if_exists == "append" else "WRITE_TRUNCATE",
    )

    client = get_client()
    job = client.load_table_from_dataframe(
        bq_df, destination_table, job_config=job_config
    )
    job.result()  # Wait for the job to complete
