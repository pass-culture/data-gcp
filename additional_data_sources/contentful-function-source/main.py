import pandas as pd
from google.cloud import bigquery

from contentful_client import ContentfulClient
from utils import (
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    ENV_SHORT_NAME,
)

CONTENTFUL_ENTRIES_TABLE_NAME = "contentful_entries"
CONTENTFUL_RELATIONSHIPS_TABLE_NAME = "contentful_relationships"


def save_modules_to_bq(modules_df, table_name):
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = bigquery_client.load_table_from_dataframe(
        modules_df, table_id, job_config=job_config
    )
    job.result()


def run(request):
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """
    contentful_envs = {"prod": "production", "stg": "testing", "dev": "testing"}
    contentful_client = ContentfulClient(env=contentful_envs[ENV_SHORT_NAME])
    modules, links = contentful_client.get_all_contentful()
    save_modules_to_bq(modules, CONTENTFUL_ENTRIES_TABLE_NAME)
    save_modules_to_bq(links, CONTENTFUL_RELATIONSHIPS_TABLE_NAME)
    return "Done"
