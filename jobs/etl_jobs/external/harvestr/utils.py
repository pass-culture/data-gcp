import os
from datetime import datetime

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager
from loguru import logger

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"

HARVESTR_MESSAGES = "harvestr_messages"
HARVESTR_MESSAGES_SCHEMA = {
    "id": "STRING",
    "client_id": "STRING",
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
    "integration_url": "STRING",
    "integration_id": "STRING",
    "title": "STRING",
    "content": "STRING",
    "channel": "STRING",
    "archived": "BOOLEAN",
    "bin": "BOOLEAN",
    "requester_id": "STRING",
    "submitter_id": "STRING",
    "labels": "STRING",
}


class SecretStr:
    """String wrapper that refuses to reveal its value in repr/str/logging."""

    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    def get_secret_value(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "SecretStr('**********')"

    def __str__(self) -> str:
        return "**********"

    def __len__(self) -> int:
        return len(self._value)


def to_sql_type(_type):
    _dict = {
        "STRING": bigquery.enums.SqlTypeNames.STRING,
        "FLOAT": bigquery.enums.SqlTypeNames.FLOAT64,
        "INTEGER": bigquery.enums.SqlTypeNames.INT64,
        "BOOLEAN": bigquery.enums.SqlTypeNames.BOOL,
        "DATETIME": bigquery.enums.SqlTypeNames.DATETIME,
        "TIMESTAMP": bigquery.enums.SqlTypeNames.TIMESTAMP,
    }
    return _dict[_type]


def save_to_bq(
    df: pd.DataFrame,
    table_name: str,
    start_date: str,
    end_date: str,
    schema_field: dict = {},
    date_column: str = "created_at",
) -> None:
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce", utc=True)
    _dates = pd.date_range(start_date, end_date)
    logger.info(f"Will Save.. {table_name} -> {df.shape[0]} rows")

    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()].copy()

        if tmp_df.shape[0] > 0:
            logger.info(f"Saving.. {table_name} -> {date_str} ({tmp_df.shape[0]} rows)")
            __save_to_bq(
                df=tmp_df,
                table_name=table_name,
                event_date=date_str,
                schema_field=schema_field,
                date_column=date_column,
            )


def __save_to_bq(
    df: pd.DataFrame,
    table_name: str,
    event_date: str,
    schema_field: dict = {},
    date_column: str = "created_at",
) -> None:
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        schema=[
            bigquery.SchemaField(col, to_sql_type(_type))
            for col, _type in schema_field.items()
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=date_column,
        ),
    )

    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


API_TOKEN = SecretStr(
    access_secret_data(
        GCP_PROJECT_ID, f"harvestr-api-token-{ENV_SHORT_NAME}", version_id="latest"
    )
    or ""
)
