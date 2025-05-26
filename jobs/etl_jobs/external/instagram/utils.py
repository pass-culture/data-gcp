import os
from datetime import datetime

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def save_multiple_partitions_to_bq(
    df, table_name, start_date, end_date, date_column, schema=None
):
    if df.shape[0] > 0:
        df[date_column] = pd.to_datetime(df[date_column])
        _dates = pd.date_range(start_date, end_date)
        print(f"Will Save.. {table_name} -> {df.shape[0]}")
        for event_date in _dates:
            date_str = event_date.strftime("%Y-%m-%d")
            tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()]
            tmp_df.loc[:, date_column] = tmp_df[date_column].astype(str)
            if tmp_df.shape[0] > 0:
                print(f"Saving.. {table_name} -> {date_str}")
                df_to_bq(tmp_df, table_name, date_str, date_column, schema)


def df_to_bq(df, table_name, event_date, date_column="export_date", schema=None):
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
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=date_column,
        ),
        schema=schema,
    )

    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
INSTAGRAM_POST_DETAIL = "instagram_post_detail"

INSTAGRAM_POST_DETAIL_DTYPE = [
    bigquery.SchemaField(
        "post_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "media_type", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "caption", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "media_url", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "permalink", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "posted_at", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("url_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField("shares", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "comments", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField("likes", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField("saved", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "video_views", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "total_interactions", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField("reach", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "impressions", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField("follows", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
    bigquery.SchemaField(
        "profile_visits", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "profile_activity", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "export_date", bigquery.enums.SqlTypeNames.TIMESTAMP, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "account_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
]

INSTAGRAM_ACCOUNT_DAILY_ACTIVITY = "instagram_account_daily_activity"

INSTAGRAM_ACCOUNT_DAILY_ACTIVITY_DTYPE = [
    bigquery.SchemaField(
        "event_date", bigquery.enums.SqlTypeNames.TIMESTAMP, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "impressions", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("views", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField("reach", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"),
    bigquery.SchemaField(
        "follower_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "email_contacts", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "phone_call_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "text_message_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "get_directions_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "website_clicks", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "profile_views", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "account_id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
]
INSTAGRAM_ACCOUNT_INSIGHTS = "instagram_account"

INSTAGRAM_ACCOUNT_INSIGHTS_DTYPE = [
    bigquery.SchemaField(
        "followers_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "follows_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "media_count", bigquery.enums.SqlTypeNames.INTEGER, mode="NULLABLE"
    ),
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "biography", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"),
    bigquery.SchemaField(
        "username", bigquery.enums.SqlTypeNames.STRING, mode="NULLABLE"
    ),
    bigquery.SchemaField(
        "export_date", bigquery.enums.SqlTypeNames.TIMESTAMP, mode="NULLABLE"
    ),
]

ACCESS_TOKEN = access_secret_data(
    GCP_PROJECT_ID, f"facebook-access-token-{ENV_SHORT_NAME}", version_id="latest"
)
INSTAGRAM_ACCOUNTS_ID = ["17841410129457081", "17841463525422101"]
