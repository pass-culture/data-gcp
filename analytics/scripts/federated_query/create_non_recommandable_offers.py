import sys

from google.cloud import bigquery

from analytics.config import GCP_PROJECT_ID

BIGQUERY_POC_DATASET = "public"

from analytics.utils import run_query
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main():
    client = bigquery.Client()

    # Define destination table
    job_config = bigquery.QueryJobConfig()
    job_config.destination = (
        f"{GCP_PROJECT_ID}.{BIGQUERY_POC_DATASET}.non_recommandable_offers"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"

    # Define query
    query = f"""
        select userId, offerId 
        from public.booking a 
        left join (select id, offerId from public.stock) b
        on a.stockId = b.id 
        where isActive=true and isCancelled=false;
    """

    # Define and launch job
    run_query(bq_client=client, query=query, job_config=job_config)


if __name__ == "__main__":
    set_env_vars()
    main()
