import pandas as pd
from google.cloud import bigquery

from contentful_client import ContentfulClient
from utils import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    GCP_PROJECT,
    ENV_SHORT_NAME,
)
from datetime import datetime

CONTENTFUL_ENTRIES_TABLE_NAME = "contentful_entries"
CONTENTFUL_RELATIONSHIPS_TABLE_NAME = "contentful_relationships"
CONTENTFUL_TAGS_TABLE_NAME = "contentful_tags"


def save_raw_modules_to_bq(modules_df, table_name):
    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    modules_df["execution_date"] = _now
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
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
    modules, links, tags = contentful_client.get_all_playlists()
    save_raw_modules_to_bq(modules.drop_duplicates(), CONTENTFUL_ENTRIES_TABLE_NAME)
    save_raw_modules_to_bq(links.drop_duplicates(), CONTENTFUL_RELATIONSHIPS_TABLE_NAME)
    save_raw_modules_to_bq(tags.drop_duplicates(), CONTENTFUL_TAGS_TABLE_NAME)

    return "Done"
