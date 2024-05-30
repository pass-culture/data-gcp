import pandas as pd
from google.cloud import bigquery

from contentful_client import ContentfulClient
from utils import (
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    ENV_SHORT_NAME,
    ENTRIES_DTYPE,
    TOKEN,
    PREVIEW_TOKEN,
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


def run():
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """
    contentful_envs = {
        "prod": {
            "env": ["production"],
            "access_token": PREVIEW_TOKEN,
            "api_url": "preview.contentful.com",
        },
        "stg": {
            "env": ["testing"],
            "access_token": TOKEN,
            "api_url": "cdn.contentful.com",
        },
        "dev": {
            "env": ["testing"],
            "access_token": TOKEN,
            "api_url": "cdn.contentful.com",
        },
    }
    modules_dfs, links_dfs, tags_dfs = [], [], []
    for contentful_env in contentful_envs[ENV_SHORT_NAME]:
        contentful_client = ContentfulClient(env=contentful_env)
        modules, links, tags = contentful_client.get_all_playlists()
        modules_dfs.append(modules)
        links_dfs.append(links)
        tags_dfs.append(tags)

    df_modules = pd.concat(modules_dfs, ignore_index=True)
    for k, v in ENTRIES_DTYPE.items():
        if k in df_modules.columns:
            df_modules[k] = df_modules[k].astype(v)

    save_raw_modules_to_bq(
        df_modules.drop_duplicates(),
        CONTENTFUL_ENTRIES_TABLE_NAME,
    )
    save_raw_modules_to_bq(
        pd.concat(links_dfs, ignore_index=True).drop_duplicates(),
        CONTENTFUL_RELATIONSHIPS_TABLE_NAME,
    )
    save_raw_modules_to_bq(
        pd.concat(tags_dfs, ignore_index=True).drop_duplicates(),
        CONTENTFUL_TAGS_TABLE_NAME,
    )

    return "Done"


run()
