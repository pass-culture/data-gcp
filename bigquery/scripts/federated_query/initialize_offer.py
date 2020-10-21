import sys

from google.cloud import bigquery

from bigquery.config import GCP_PROJECT_ID, GCP_REGION, BIGQUERY_POC_DATASET, CLOUDSQL_DATABASE
from set_env import set_env_vars

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


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
    logger.info(f"Query running: < {query_job.query} >")
    results = query_job.result()
    elapsed_time = round((query_job.ended - query_job.created).total_seconds(), 2)
    Mb_processed = round(query_job.total_bytes_processed / 1000000, 2)
    logger.info(f"Inserted {results.total_rows} lines ({Mb_processed} Mb) in {elapsed_time} sec")


if __name__ == "__main__":
    set_env_vars()
    main()
