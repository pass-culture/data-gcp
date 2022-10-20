import os
from datetime import datetime
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager, bigquery


PROJECT_NAME = os.environ.get("PROJECT_NAME")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")


def to_sql_type(_type):
    _dict = {
        str: bigquery.enums.SqlTypeNames.STRING,
        float: bigquery.enums.SqlTypeNames.FLOAT64,
        int: bigquery.enums.SqlTypeNames.INT64,
        bool: bigquery.enums.SqlTypeNames.BOOL,
    }
    return _dict[_type]


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def save_to_bq(df, table_name, execution_date, schema_field):
    _now = datetime.strptime(execution_date, "%Y-%m-%d")
    yyyymmdd = _now.strftime("%Y%m%d")
    df["execution_date"] = _now
    bigquery_client = bigquery.Client()
    table_id = f"{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
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
