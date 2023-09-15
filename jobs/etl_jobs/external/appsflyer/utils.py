import os
from datetime import datetime
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager, bigquery
import pandas as pd

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENVIRONMENT_SHORT_NAME}"


def to_sql_type(_type):
    _dict = {
        str: bigquery.enums.SqlTypeNames.STRING,
        float: bigquery.enums.SqlTypeNames.FLOAT64,
        int: bigquery.enums.SqlTypeNames.INT64,
        bool: bigquery.enums.SqlTypeNames.BOOL,
    }
    return _dict[_type]


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def save_to_bq(df, table_name, start_date, end_date, schema_field, date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    _dates = pd.date_range(start_date, end_date)
    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str)]
        tmp_df[date_column] = tmp_df[date_column].astype(str)
        if tmp_df.shape[0] > 0:
            print(f"Saving.. {table_name} -> {date_str}")
            __save_to_bq(tmp_df, date_str, table_name, schema_field)


def __save_to_bq(df, event_date, table_name, schema_field):
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")
    df["event_date"] = date_fmt
    bigquery_client = bigquery.Client()
    table_id = f"{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_date",
        ),
        schema=[
            bigquery.SchemaField(col, to_sql_type(_type))
            for col, _type in schema_field.items()
        ],
    )

    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


TOKEN = access_secret_data(PROJECT_NAME, f"appsflyer_token_{ENVIRONMENT_SHORT_NAME}")
IOS_APP_ID = access_secret_data(
    PROJECT_NAME, f"appsflyer_ios_app_id_{ENVIRONMENT_SHORT_NAME}"
)
ANDROID_APP_ID = access_secret_data(
    PROJECT_NAME, f"appsflyer_android_app_id_{ENVIRONMENT_SHORT_NAME}"
)
