import sys

from google.cloud import bigquery

from analytics.config import (
    GCP_PROJECT_ID,
    GCP_REGION,
    CLOUDSQL_DATABASE,
    MIGRATION_ENRICHED_VENUE_DATA,
    MIGRATION_ENRICHED_OFFERER_DATA,
    MIGRATION_ENRICHED_USER_DATA,
    MIGRATION_ENRICHED_OFFER_DATA,
    MIGRATION_ENRICHED_STOCK_DATA,
    MIGRATION_ENRICHED_BOOKING_DATA,
    BIGQUERY_POC_DATASET,
    ANONYMIZATION_TABLES,
    ENRICHED_USER_DATA_TABLES,
    ENRICHED_OFFERER_DATA_TABLES,
    ENRICHED_VENUE_DATA_TABLES,
    ENRICHED_OFFER_DATA_TABLES,
    ENRICHED_STOCK_DATA_TABLES,
    ENRICHED_BOOKING_DATA_TABLES
)

from analytics.utils import run_query
from dependencies.data_analytics.import_tables import define_import_query
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main(tables, dataset):
    client = bigquery.Client()

    # define destination table
    job_config = bigquery.QueryJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"

    # define and launch jobs
    for table in tables:
        query = define_import_query(
            table, region=GCP_REGION, cloudsql_database=CLOUDSQL_DATABASE
        )
        job_config.destination = f"{GCP_PROJECT_ID}.{dataset}.{table}"
        run_query(bq_client=client, query=query, job_config=job_config)


if __name__ == "__main__":
    set_env_vars()

    main(tables=ANONYMIZATION_TABLES, dataset=BIGQUERY_POC_DATASET)
    main(tables=ENRICHED_USER_DATA_TABLES, dataset=MIGRATION_ENRICHED_USER_DATA)
    main(tables=ENRICHED_OFFERER_DATA_TABLES, dataset=MIGRATION_ENRICHED_OFFERER_DATA)
    main(tables=ENRICHED_VENUE_DATA_TABLES, dataset=MIGRATION_ENRICHED_VENUE_DATA)
    main(tables=ENRICHED_OFFER_DATA_TABLES, dataset=MIGRATION_ENRICHED_OFFER_DATA)
    main(tables=ENRICHED_STOCK_DATA_TABLES, dataset=MIGRATION_ENRICHED_STOCK_DATA)
    main(tables=ENRICHED_BOOKING_DATA_TABLES, dataset=MIGRATION_ENRICHED_BOOKING_DATA)
