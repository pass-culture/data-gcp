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


def save_to_bq(df, table_name, event_date, date_column="export_date"):
    date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
    yyyymmdd = date_fmt.strftime("%Y%m%d")

    df[date_column] = pd.to_datetime(date_fmt).date()

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

    # Load the DataFrame into BigQuery
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
ENV_SHORT_NAME = os.environ["ENV_SHORT_NAME"]
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"


ZENDESK_API_EMAIL = access_secret_data(
    GCP_PROJECT_ID, f"zendesk-api-email-{ENV_SHORT_NAME}", version_id="latest"
)
ZENDESK_API_KEY = access_secret_data(
    GCP_PROJECT_ID, f"zendesk-api-key-{ENV_SHORT_NAME}", version_id="latest"
)
ZENDESK_SUBDOMAIN = "passculture"
