from datetime import datetime

import typer
from google.cloud import bigquery

from contentful_client import ContentfulClient
from utils import (
    BIGQUERY_RAW_DATASET,
    ENTRIES_DTYPE,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    PREVIEW_TOKEN,
    TOKEN,
)

CONTENTFUL_ENTRIES_TABLE_NAME = "contentful_entry"
CONTENTFUL_RELATIONSHIP_TABLE_NAME = "contentful_relationship"
CONTENTFUL_TAG_TABLE_NAME = "contentful_tag"


def save_raw_modules_to_bq(modules_df, table_name):
    nb_rows = modules_df.shape[0]
    if nb_rows == 0:
        print(f"No rows to save for {table_name}")
        return

    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    modules_df["execution_date"] = _now
    print(f"Will save {nb_rows} rows to {table_name}")

    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
    )
    job = bigquery_client.load_table_from_dataframe(
        modules_df, table_id, job_config=job_config
    )
    job.result()


def run(
    playlists_names: str = typer.Option(None, help="Liste des playlists à importer"),
):
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """
    contentful_envs = {
        "prod": {
            "env": "production",
            "access_token": PREVIEW_TOKEN,
            "api_url": "preview.contentful.com",
        },
        "stg": {
            "env": "testing",
            "access_token": TOKEN,
            "api_url": "cdn.contentful.com",
        },
        "dev": {
            "env": "testing",
            "access_token": TOKEN,
            "api_url": "cdn.contentful.com",
        },
    }

    playlists_names = playlists_names.split(",") if playlists_names else []

    config_env = contentful_envs[ENV_SHORT_NAME]
    contentful_client = ContentfulClient(config_env, playlists_names)
    df_modules, links_df, tags_df = contentful_client.get_playlists()

    for k, v in ENTRIES_DTYPE.items():
        if k in df_modules.columns:
            df_modules[k] = df_modules[k].astype(v)

    save_raw_modules_to_bq(
        df_modules.drop_duplicates(),
        CONTENTFUL_ENTRIES_TABLE_NAME,
    )
    save_raw_modules_to_bq(
        links_df.drop_duplicates(),
        CONTENTFUL_RELATIONSHIP_TABLE_NAME,
    )
    save_raw_modules_to_bq(
        tags_df.drop_duplicates(),
        CONTENTFUL_TAG_TABLE_NAME,
    )

    return "Done"


if __name__ == "__main__":
    typer.run(run)
