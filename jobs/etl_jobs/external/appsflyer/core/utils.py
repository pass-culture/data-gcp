import io
import os
from datetime import datetime

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
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
    print(f"Will Save.. {table_name} -> {df.shape[0]}")
    for event_date in _dates:
        date_str = event_date.strftime("%Y-%m-%d")
        tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()]
        tmp_df[date_column] = tmp_df[date_column].astype(str)
        if tmp_df.shape[0] > 0:
            print(f"Saving.. {table_name} -> {date_str}")
            __save_to_bq(tmp_df, date_str, table_name, schema_field)


def __save_to_bq(df, event_date, table_name, schema_field):
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")
    df.loc[:, "event_date"] = date_fmt
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
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
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
    )

    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def export_polars_to_bq(
    data, output_table, event_date, partition_date="execution_date"
):
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")
    client = bigquery.Client()
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=f"{BIGQUERY_RAW_DATASET}.{output_table}${yyyymmdd}",
            project=GCP_PROJECT_ID,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_date,
                ),
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
            ),
        )
    job.result()


TOKEN = access_secret_data(GCP_PROJECT_ID, f"appsflyer_token_{ENVIRONMENT_SHORT_NAME}")
IOS_APP_ID = access_secret_data(
    GCP_PROJECT_ID, f"appsflyer_ios_app_id_{ENVIRONMENT_SHORT_NAME}"
)
ANDROID_APP_ID = access_secret_data(
    GCP_PROJECT_ID, f"appsflyer_android_app_id_{ENVIRONMENT_SHORT_NAME}"
)
