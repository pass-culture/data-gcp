import os

import pandas as pd
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager
from loguru import logger

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"

APPFOLLOW_REVIEWS = "appfollow_reviews"
APPFOLLOW_REVIEWS_SCHEMA = {
    "review_id": "STRING",
    "date": "DATETIME",
    "time": "STRING",
    "title": "STRING",
    "rating": "INTEGER",
    "content": "STRING",
    "store": "STRING",
    "answer_text": "STRING",
    "answer_date": "STRING",
    "app_version": "STRING",
    "ext_id": "STRING",
}

# One row per date x ext_id x country. Values are cumulative (all-time) totals
# as returned by AppFollow /meta/ratings/history with type=total.
APPFOLLOW_RATINGS = "appfollow_ratings"
APPFOLLOW_RATINGS_SCHEMA = {
    "date": "DATE",
    "ext_id": "STRING",
    "store": "STRING",
    "country": "STRING",
    "rating_avg": "FLOAT",
    "ratings_total": "INTEGER",
    "stars_1_total": "INTEGER",
    "stars_2_total": "INTEGER",
    "stars_3_total": "INTEGER",
    "stars_4_total": "INTEGER",
    "stars_5_total": "INTEGER",
    "imported_at": "TIMESTAMP",
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
        "DATE": bigquery.enums.SqlTypeNames.DATE,
        "DATETIME": bigquery.enums.SqlTypeNames.DATETIME,
        "TIMESTAMP": bigquery.enums.SqlTypeNames.TIMESTAMP,
    }
    return _dict[_type]


def infer_store(ext_id: str) -> str:
    """AppFollow store code from an app external ID: numeric IDs are App Store apps."""
    return "as" if ext_id.isdigit() else "gp"


def replace_app_rows_in_bq(
    df: pd.DataFrame,
    table_name: str,
    schema_field: dict,
    start_date: str,
    end_date: str,
    ext_id: str,
    date_column: str = "date",
) -> None:
    """
    Idempotently replace the rows of one app over a date window.

    Deletes every row of `ext_id` whose `date_column` falls in [start_date, end_date],
    then appends `df`. Unlike a WRITE_TRUNCATE on a `table$yyyymmdd` partition, this
    does not erase the rows of the other apps sharing the same day partition, and it
    also removes rows that are no longer returned by the API (e.g. deleted reviews).
    """
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{table_name}"

    try:
        bigquery_client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    if table_exists:
        logger.info(
            f"Deleting {table_name} rows for {ext_id} between {start_date} and {end_date}"
        )
        delete_job = bigquery_client.query(
            f"""
            DELETE FROM `{table_id}`
            WHERE DATE({date_column}) BETWEEN @start_date AND @end_date
              AND ext_id = @ext_id
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                    bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                    bigquery.ScalarQueryParameter("ext_id", "STRING", ext_id),
                ]
            ),
        )
        delete_job.result()
        logger.info(f"Deleted {delete_job.num_dml_affected_rows} rows")

    if df.empty:
        logger.warning(f"No {table_name} rows to load for {ext_id}")
        return

    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])
    if schema_field.get(date_column) == "DATE":
        df[date_column] = df[date_column].dt.date

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
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
    logger.info(f"Appending {df.shape[0]} rows to {table_name}")
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


def get_api_token() -> SecretStr:
    return SecretStr(
        access_secret_data(
            GCP_PROJECT_ID, f"appfollow-api-token-{ENV_SHORT_NAME}", version_id="latest"
        )
        or ""
    )
