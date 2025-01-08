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


def save_multiple_partitions_to_bq(df, table_name, start_date, end_date, date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    _dates = pd.date_range(start_date, end_date)
    print(f"Will Save.. {table_name} -> {df.shape[0]}")
    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()]
        tmp_df.loc[:, date_column] = tmp_df[date_column].astype(str)
        if tmp_df.shape[0] > 0:
            print(f"Saving.. {table_name} -> {date_str}")
            df_to_bq(tmp_df, table_name, date_str, date_column)


def df_to_bq(df, table_name, event_date, date_column="export_date"):
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
    )

    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
INSTAGRAM_POST_DETAIL = "instagram_post_detail"
INSTAGRAM_ACCOUNT_DAILY_ACTIVITY = "instagram_account_daily_activity"
INSTAGRAM_ACCOUNT_INSIGHTS = "instagram_account"

ACCESS_TOKEN = access_secret_data(
    GCP_PROJECT_ID, f"facebook-access-token-{ENV_SHORT_NAME}", version_id="latest"
)
ACCOUNT_ID = "17841410129457081"
