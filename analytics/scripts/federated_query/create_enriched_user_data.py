import sys

from google.cloud import bigquery

from analytics.utils import run_query
from analytics.config import MIGRATION_ENRICHED_USER_DATA
from dependencies.data_analytics.enriched_data.user import (
    define_enriched_user_data_full_query,
)
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main(dataset):
    client = bigquery.Client()

    # Define query
    overall_query = define_enriched_user_data_full_query(dataset=dataset)

    # Run query
    run_query(bq_client=client, query=overall_query)


if __name__ == "__main__":
    set_env_vars()
    main(dataset=MIGRATION_ENRICHED_USER_DATA)
