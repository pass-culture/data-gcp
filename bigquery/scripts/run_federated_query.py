import sys

from google.cloud import bigquery

from bigquery.config import GCP_PROJECT_ID, GCP_REGION, BIGQUERY_POC_DATASET, CLOUDSQL_DATABASE
from set_env import set_env_vars

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    client = bigquery.Client()

    # define destination table
    job_config = bigquery.QueryJobConfig()
    job_config.destination = f"{GCP_PROJECT_ID}.{BIGQUERY_POC_DATASET}.offer"
    job_config.write_disposition = "WRITE_TRUNCATE"

    # define query
    query = f"SELECT * FROM EXTERNAL_QUERY('{GCP_REGION}.{CLOUDSQL_DATABASE}', 'SELECT * FROM offer');"

    # define and launch job
    query_job = client.query(query=query, job_config=job_config)
    results = query_job.result()


if __name__ == "__main__":
    set_env_vars()
    main()
