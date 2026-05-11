import os
from datetime import datetime
from typing import List, Optional

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager


def access_secret_data(
    project_id: str, secret_id: str, default: Optional[str] = None
) -> Optional[str]:
    """
    Access a secret value from Google Cloud Secret Manager.

    Args:
        project_id (str): The GCP project ID.
        secret_id (str): The secret ID to retrieve.
        default (Optional[str]): Default value to return if access fails.

    Returns:
        Optional[str]: The secret value as a string, or the default value if access fails.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def save_multiple_partitions_to_bq(
    df: pd.DataFrame,
    table_name: str,
    schema_field: List[bigquery.SchemaField],
    date_column: str = "updated_date",
) -> None:
    """
    Save a DataFrame to BigQuery, partitioned by dates.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the BigQuery table.
        schema_field (List[bigquery.SchemaField]): Schema for the BigQuery table.
        date_column (str): The column used for partitioning dates.
    """
    start_date = min(df[date_column])
    end_date = max(df[date_column])
    _dates = pd.date_range(start_date, end_date)
    print(f"Will Save.. {table_name} -> {df.shape[0]} rows")

    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column] == pd.to_datetime(date_str).date()]
        tmp_df[date_column] = tmp_df[date_column].astype(str)
        if tmp_df.shape[0] > 0:
            print(f"Saving.. {table_name} -> {date_str}")
            save_to_bq(
                df=tmp_df,
                table_name=table_name,
                schema_field=schema_field,
                event_date=date_str,
                date_column=date_column,
            )


def save_to_bq(
    df: pd.DataFrame,
    table_name: str,
    schema_field: List[bigquery.SchemaField],
    event_date: str,
    date_column: str = "export_date",
) -> None:
    """
    Save a DataFrame to a BigQuery table.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the BigQuery table.
        schema_field (List[bigquery.SchemaField]): Schema for the BigQuery table.
        event_date (str): The event date (used for partitioning).
        date_column (str): The column used for the event date.
    """
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")

    df = df.copy()
    df[date_column] = pd.to_datetime(date_fmt).date()

    # pandas 3.x uses Arrow-backed large_string dtype; pyarrow cannot cast
    # large_string → list for REPEATED fields, so we convert them explicitly.
    # Also, BigQuery REPEATED fields cannot contain null elements, so we filter them out.
    def _clean_repeated_field(v):
        """Convert a value to a list, dropping None/NaN elements for BQ REPEATED fields."""

        def _is_null(x):
            if x is None:
                return True
            try:
                return bool(pd.isna(x))
            except (TypeError, ValueError):
                return False

        if isinstance(v, list):
            return [x for x in v if not _is_null(x)]
        elif _is_null(v):
            return []
        else:
            return [v]

    for field in schema_field:
        if field.mode == "REPEATED" and field.name in df.columns:
            df[field.name] = [_clean_repeated_field(v) for v in df[field.name]]

    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        schema=schema_field,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=date_column,
        ),
    )

    # Load the DataFrame into BigQuery
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


# Global variables
GCP_PROJECT_ID: str = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME: str = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET: str = f"raw_{ENV_SHORT_NAME}"

# Zendesk API credentials
ZENDESK_API_EMAIL: Optional[str] = access_secret_data(
    GCP_PROJECT_ID, f"zendesk-api-email-{ENV_SHORT_NAME}"
)
ZENDESK_API_KEY: Optional[str] = access_secret_data(
    GCP_PROJECT_ID, f"zendesk-api-key-{ENV_SHORT_NAME}"
)
ZENDESK_SUBDOMAIN: str = "passculture"
