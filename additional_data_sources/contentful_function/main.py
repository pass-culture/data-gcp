from datetime import date

from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError

from contentful_client import ContentfulClient
from utils import BIGQUERY_CLEAN_DATASET, GCP_PROJECT, ENV_SHORT_NAME


def update_criterion(playlists):
    bigquery_client = bigquery.Client()
    for tag in playlists:
        playlist = playlists[tag]
        query = f"""
            UPDATE `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_criterion`
            SET module_id = @module_id, 
                module_type = @module_type, 
                title =  @title,
                is_geolocated = @is_geolocated,
                around_radius = @around_radius,
                categories = @categories,
                is_digital = @is_digital,
                is_thing = @is_thing,
                is_event = @is_event,
                beginning_datetime = @beginning_datetime,
                ending_datetime = @ending_datetime,
                is_free = @is_free,
                price_min = @price_min,
                price_max = @price_max,
                is_duo = @is_duo,
                newest_only = @newest_only,
                hits_per_page = @hits_per_page,
                layout = @layout,
                min_offers = @min_offers,
                child_playlists = @child_playlists
            WHERE name= @tag
        """
        job_config = bigquery.QueryJobConfig(
            dry_run=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("tag", "STRING", playlist["tag"]),
                bigquery.ScalarQueryParameter(
                    "module_id", "STRING", playlist["module_id"]
                ),
                bigquery.ScalarQueryParameter(
                    "module_type", "STRING", playlist["module_type"]
                ),
                bigquery.ScalarQueryParameter("title", "STRING", playlist["title"]),
                bigquery.ScalarQueryParameter(
                    "is_geolocated", "BOOL", playlist["is_geolocated"]
                ),
                bigquery.ScalarQueryParameter(
                    "around_radius", "FLOAT64", playlist["around_radius"]
                ),
                bigquery.ScalarQueryParameter(
                    "categories", "STRING", playlist["categories"]
                ),
                bigquery.ScalarQueryParameter(
                    "is_digital", "BOOL", playlist["is_digital"]
                ),
                bigquery.ScalarQueryParameter("is_thing", "BOOL", playlist["is_thing"]),
                bigquery.ScalarQueryParameter("is_event", "BOOL", playlist["is_event"]),
                bigquery.ScalarQueryParameter(
                    "beginning_datetime", "TIMESTAMP", playlist["beginning_datetime"]
                ),
                bigquery.ScalarQueryParameter(
                    "ending_datetime", "TIMESTAMP", playlist["ending_datetime"]
                ),
                bigquery.ScalarQueryParameter("is_free", "BOOL", playlist["is_free"]),
                bigquery.ScalarQueryParameter(
                    "price_min", "FLOAT64", playlist["price_min"]
                ),
                bigquery.ScalarQueryParameter(
                    "price_max", "FLOAT64", playlist["price_max"]
                ),
                bigquery.ScalarQueryParameter("is_duo", "BOOL", playlist["is_duo"]),
                bigquery.ScalarQueryParameter(
                    "newest_only", "BOOL", playlist["newest_only"]
                ),
                bigquery.ScalarQueryParameter(
                    "hits_per_page", "INT64", playlist["hits_per_page"]
                ),
                bigquery.ScalarQueryParameter("layout", "STRING", playlist["layout"]),
                bigquery.ScalarQueryParameter(
                    "min_offers", "INT64", playlist["min_offers"]
                ),
                bigquery.ScalarQueryParameter(
                    "child_playlists", "STRING", playlist["child_playlists"]
                ),
            ],
        )
        query_job = bigquery_client.query(query, job_config=job_config)
        print(query_job.result())


def run(request):
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """
    contentful_envs = {"prod": "production", "stg": "testing", "dev": "testing"}
    contentful_client = ContentfulClient(env=contentful_envs[ENV_SHORT_NAME])
    playlists = contentful_client.get_algolia_modules()

    update_criterion(playlists)

    return "Done"
