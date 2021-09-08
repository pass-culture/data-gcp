import pandas as pd
from google.cloud import bigquery

from contentful_client import ContentfulClient
from utils import (
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    ENV_SHORT_NAME,
)


def insert_criterion(playlists):
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.contentful_data"
    playlists_df = pd.DataFrame(
        playlists,
        columns=[
            "tag",
            "module_id",
            "module_type",
            "title",
            "is_geolocated",
            "around_radius",
            "categories",
            "is_digital",
            "is_thing",
            "is_event",
            "beginning_datetime",
            "ending_datetime",
            "is_free",
            "price_min",
            "price_max",
            "is_duo",
            "newest_only",
            "hits_per_page",
            "layout",
            "min_offers",
            "child_playlists",
            "date_updated",
        ],
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = bigquery_client.load_table_from_dataframe(
        playlists_df, table_id, job_config=job_config
    )
    job.result()


def run(request):
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """
    contentful_envs = {"prod": "production", "stg": "testing", "dev": "testing"}
    contentful_client = ContentfulClient(env=contentful_envs[ENV_SHORT_NAME])
    playlists = contentful_client.get_algolia_modules()

    insert_criterion(playlists)

    return "Done"
