import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

import time

BIGQUERY_RAW_DATASET = os.environ.get("RAW_DATASET")
ENV_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
GCP_PROJECT = os.environ.get("PROJECT_NAME")

def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def bigquery_load_job(df, partition_date, partitioning_field, gcp_project_id, dataset, table_name, schema):
    # load in bigquery with partitioning
    yyyymmdd = partition_date.strftime("%Y%m%d")
    table_id = f"{gcp_project_id}.{dataset}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=partitioning_field
        ),
        schema=[
            bigquery.SchemaField(column, _type) for column, _type in schema.items()
        ],
    )
    job = bigquery_client.load_table_from_dataframe(
        transac_df, table_id, job_config=job_config
    )
    job.result()