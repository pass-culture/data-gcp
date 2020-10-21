import sys

from google.cloud import bigquery

from bigquery.config import GCP_PROJECT
from set_env import set_env_vars

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def main():
    client = bigquery.Client()

    # define destination table
    job_config = bigquery.QueryJobConfig()
    job_config.destination = f"{GCP_PROJECT}.poc_data_federated_query.offer"
    job_config.write_disposition = "WRITE_TRUNCATE"

    # define query
    query = "SELECT * FROM EXTERNAL_QUERY('europe-west1.cloud_SQL_dump-prod-8-10-2020', 'SELECT * FROM offer');"

    # define and launch job
    query_job = client.query(query=query, job_config=job_config)
    results = query_job.result()


if __name__ == "__main__":
    set_env_vars()
    main()
