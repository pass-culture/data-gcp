import logging
import sys

from google.cloud import bigquery

from bigquery.config import GCP_PROJECT_ID
from set_env import set_env_vars

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

BIGQUERY_POC_DATASET = "public"
BUCKET_PATH = "gs://pass-culture-data/bigquery_exports"
TABLE_NAME = "non_recommandable_offers"
SQL_INSTANCE = "pcdata-poc-csql-recommendation"
SQL_BASE = "pcdata-poc-csql-recommendation"


def main():
    client = bigquery.Client()
    logger.info(f"\t{BIGQUERY_POC_DATASET}.{TABLE_NAME} EXPORT STARTTED")
    table_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BIGQUERY_POC_DATASET).table(
        TABLE_NAME
    )

    extract_job = client.extract_table(
        table_ref,
        destination_uris=f"{BUCKET_PATH}/{TABLE_NAME}.csv",
        job_config=bigquery.job.ExtractJobConfig(print_header=False),
    )
    extract_job.result()
    logger.info(f"\t{BIGQUERY_POC_DATASET}.{TABLE_NAME} EXPORT SUCCEEDED")

    schema = client.get_table(table_ref).schema
    columns = ", ".join([f"{col.name} {col.field_type}" for col in schema])

    logger.info(
        f"\tTable {BIGQUERY_POC_DATASET}.{TABLE_NAME} exported to {BUCKET_PATH}/{TABLE_NAME}.csv\n"
    )

    logger.info(
        f"\tRun these commands in cloudshell to create the table in CloudSQL:\n\n"
        f"\tgcloud config set project pass-culture-app-projet-test\n"
        f"\tgcloud sql connect {SQL_INSTANCE}\n"
        f"\t\connect {SQL_BASE}\n"
        f"\tCREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns});\n\n"
        f"And then, in another cloudshell:\n"
        f"\tgcloud sql import csv {SQL_INSTANCE} {BUCKET_PATH}/{TABLE_NAME}.csv --database={SQL_BASE} --table={TABLE_NAME}"
    )


if __name__ == "__main__":
    set_env_vars()
    main()
