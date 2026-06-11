import os
from datetime import datetime

import pandas as pd
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
    "ext_id": "STRING",
}


def to_sql_type(_type):
    _dict = {
        "STRING": bigquery.enums.SqlTypeNames.STRING,
        "FLOAT": bigquery.enums.SqlTypeNames.FLOAT64,
        "INTEGER": bigquery.enums.SqlTypeNames.INT64,
        "DATETIME": bigquery.enums.SqlTypeNames.DATETIME,
        "TIMESTAMP": bigquery.enums.SqlTypeNames.TIMESTAMP,
    }
    return _dict[_type]


def save_to_bq(
    df, table_name, start_date, end_date, schema_field={}, date_column="date"
):
    df[date_column] = pd.to_datetime(df[date_column])
    _dates = pd.date_range(start_date, end_date)
    logger.info(f"Will Save.. {table_name} -> {df.shape[0]}")
    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()].copy()

        if tmp_df.shape[0] > 0:
            logger.info(f"Saving.. {table_name} -> {date_str}")
            __save_to_bq(
                df=tmp_df,
                table_name=table_name,
                event_date=date_str,
                schema_field=schema_field,
                date_column=date_column,
            )


def __save_to_bq(df, table_name, event_date, schema_field={}, date_column="date"):
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")
    df.loc[:, date_column] = date_fmt
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


API_TOKEN = access_secret_data(
    GCP_PROJECT_ID, f"appfollow-api-token-{ENV_SHORT_NAME}", version_id="latest"
)
