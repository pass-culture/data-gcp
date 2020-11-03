import sys

from google.cloud import bigquery

from bigquery.config import GCP_PROJECT_ID, GCP_REGION, CLOUDSQL_DATABASE
from bigquery.utils import run_query
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def main(tables, dataset):
    client = bigquery.Client()

    # define destination table
    job_config = bigquery.QueryJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"

    # define query
    queries = {}
    queries["user"] = f"SELECT * FROM EXTERNAL_QUERY('{GCP_REGION}.{CLOUDSQL_DATABASE}', 'SELECT \"id\", \"validationToken\", \"email\", \"password\", \"publicName\", \"dateCreated\", \"departementCode\", \"canBookFreeOffers\", \"isAdmin\", \"resetPasswordToken\", \"resetPasswordTokenValidityLimit\", \"firstName\", \"lastName\", \"postalCode\", \"phoneNumber\", \"dateOfBirth\", \"needsToFillCulturalSurvey\", CAST(\"culturalSurveyId\" AS varchar(255)), \"civility\", \"activity\", \"culturalSurveyFilledDate\", \"hasSeenTutorials\", \"address\", \"city\", \"lastConnectionDate\" FROM public.user');"
    queries["user_offerer"] = f"SELECT * FROM EXTERNAL_QUERY('{GCP_REGION}.{CLOUDSQL_DATABASE}', 'SELECT \"id\", \"userId\", \"offererId\", CAST(\"rights\" AS varchar(255)), \"validationToken\" FROM public.user_offerer');"


    # define and launch jobs
    for table in tables:
        if table not in queries:
            query = f"SELECT * FROM EXTERNAL_QUERY('{GCP_REGION}.{CLOUDSQL_DATABASE}', 'SELECT * FROM {table}');"
        else:
            query = queries[table]
        job_config.destination = f"{GCP_PROJECT_ID}.{dataset}.{table}"
        run_query(bq_client=client, query=query, job_config=job_config)


if __name__ == "__main__":
    set_env_vars()
    enriched_offerer_data_tables = ["offerer", "venue", "offer", "stock", "booking"]
    main(tables=enriched_offerer_data_tables, dataset="migration_enriched_offerer_data")
