import os
from datetime import datetime

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager
from loguru import logger

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
# export tables
TIKTOK_VIDEO_DETAIL = "tiktok_video_detail"
TIKTOK_VIDEO_DETAIL_SCHEMA = {
    "item_id": "STRING",
    "caption": "STRING",
    "average_time_watched": "FLOAT",
    "reach": "INTEGER",
    "create_time": "STRING",
    "share_url": "STRING",
    "video_views": "INTEGER",
    "total_time_watched": "FLOAT",
    "likes": "INTEGER",
    "shares": "INTEGER",
    "comments": "INTEGER",
    "video_duration": "FLOAT",
    "full_video_watched_rate": "FLOAT",
    "thumbnail_url": "STRING",
    "embed_url": "STRING",
    "account": "STRING",
    "export_date": "DATETIME",
}
TIKTOK_VIDEO_AUDIENCE_COUNTRY = "tiktok_video_audience_country"
TIKTOK_VIDEO_AUDIENCE_COUNTRY_SCHEMA = {
    "item_id": "STRING",
    "country": "STRING",
    "percentage": "FLOAT",
    "account": "STRING",
    "export_date": "DATETIME",
}

TIKTOK_VIDEO_IMPRESSION_SOURCE = "tiktok_video_impression_source"
TIKTOK_VIDEO_IMPRESSION_SOURCE_SCHEMA = {
    "item_id": "STRING",
    "impression_source": "STRING",
    "percentage": "FLOAT",
    "account": "STRING",
    "export_date": "DATETIME",
}

TIKTOK_ACCOUNT_HOURLY_AUDIENCE = "tiktok_account_hourly_audience"
TIKTOK_ACCOUNT_DAILY_ACTIVITY = "tiktok_account_daily_activity"


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def to_sql_type(_type):
    _dict = {
        "STRING": bigquery.enums.SqlTypeNames.STRING,
        "FLOAT": bigquery.enums.SqlTypeNames.FLOAT64,
        "INTEGER": bigquery.enums.SqlTypeNames.INT64,
        "DATETIME": bigquery.enums.SqlTypeNames.DATETIME,
    }
    return _dict[_type]


def save_to_bq(
    df, table_name, start_date, end_date, schema_field={}, date_column="export_date"
):
    df[date_column] = pd.to_datetime(df[date_column])
    _dates = pd.date_range(start_date, end_date)
    logger.info(f"Will Save.. {table_name} -> {df.shape[0]}")
    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()]
        tmp_df.loc[:, date_column] = tmp_df[date_column].astype(str)
        if tmp_df.shape[0] > 0:
            logger.info(f"Saving.. {table_name} -> {date_str}")
            __save_to_bq(
                df=tmp_df,
                table_name=table_name,
                event_date=date_str,
                schema_field=schema_field,
                date_column=date_column,
            )


def __save_to_bq(
    df, table_name, event_date, schema_field={}, date_column="export_date"
):
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


REFRESH_TOKEN = access_secret_data(
    GCP_PROJECT_ID, f"tiktok-refresh-token-{ENV_SHORT_NAME}", version_id="latest"
)
CLIENT_ID = access_secret_data(
    GCP_PROJECT_ID, f"tiktok-client-id-{ENV_SHORT_NAME}", version_id="latest"
)
CLIENT_SECRET = access_secret_data(
    GCP_PROJECT_ID, f"tiktok-client-secret-{ENV_SHORT_NAME}", version_id="latest"
)
