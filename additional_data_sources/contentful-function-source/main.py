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


def save_clean_modules_to_bq(table_name, id_key):
    bigquery_client = bigquery.Client()

    df = bigquery_client.query(
        f"""
        SELECT * except(row_number)
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY execution_date DESC) as row_number
            FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}`
        ) inn
        WHERE row_number=1
        """
    ).to_dataframe()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bigquery_client.load_table_from_dataframe(
        df,
        f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{table_name}",
        job_config=job_config,
    )
    job.result()


def save_clean_entries_to_bq(table_name, id_key):
    bigquery_client = bigquery.Client()

    df = bigquery_client.query(
        f"""
         SELECT  * except(row_number, ending_datetime,beginning_datetime, is_geolocated, is_duo, is_free,is_event,is_thing, price_max, min_offers )
        ,TIMESTAMP(ending_datetime) AS ending_datetime
        ,TIMESTAMP(beginning_datetime) AS beginning_datetime
        , CAST(is_geolocated AS bool) AS is_geolocated
        , CAST(is_duo AS bool) AS is_duo
        , CAST(is_free AS bool) AS is_free
        , CAST(is_event AS bool) AS is_event
        , CAST(is_thing AS bool) AS is_thing
        , CAST(price_max AS FLOAT64) AS price_max
        , CAST(min_offers AS INT64) AS min_offers
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {id_key} ORDER BY execution_date DESC) as row_number
            FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}`
        ) inn
        WHERE row_number=1
        """
    ).to_dataframe()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bigquery_client.load_table_from_dataframe(
        df,
        f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{table_name}",
        job_config=job_config,
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

    save_clean_entries_to_bq(CONTENTFUL_ENTRIES_TABLE_NAME, id_key="id")
    save_clean_modules_to_bq(CONTENTFUL_RELATIONSHIPS_TABLE_NAME, id_key="parent,child")
    save_clean_modules_to_bq(CONTENTFUL_TAGS_TABLE_NAME, id_key="tag_id,entry_id")

    return "Done"
