import sys

from google.cloud import bigquery

from bigquery.utils import run_query
from bigquery.config import MIGRATION_ENRICHED_OFFERER_DATA
from dependencies.data_analytics.enriched_data.offerer import (
    define_enriched_offerer_data_full_query,
)
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main(dataset):
    client = bigquery.Client()

    # Define query
    overall_query = define_enriched_offerer_data_full_query(dataset=dataset)

    # Run query
    run_query(bq_client=client, query=overall_query)


if __name__ == "__main__":
    set_env_vars()
    main(dataset=MIGRATION_ENRICHED_OFFERER_DATA)
