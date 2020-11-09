import logging
import sys

from bigquery.config import (
    GCP_PROJECT_ID,
)
from set_env import set_env_vars

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

BUCKET_PATH = 'gs://pass-culture-data'
SQL_DUMP = 'dump_staging_for_recommendations_09_11_20.gz'
SQL_INSTANCE = 'pcdata-poc-csql-recommendation'
SQL_BASE = 'pcdata-poc-csql-recommendation'


def main():
    logger.info(f"\tRun this commands in cloudshell (or your terminal if you have gcloud SDK installed) "
                f"to restore data in CloudSQL from {BUCKET_PATH}/{SQL_DUMP}:\n\n"
                f"\tgcloud sql connect {SQL_INSTANCE}\n"
                f"\t\connect {SQL_BASE}\n"
                f"\tDROP SCHEMA public CASCADE;"
                f"\tCREATE SCHEMA IF EXISTS public;"
                f"\tThen in an other terminal:"
                f"\tgcloud config set project {GCP_PROJECT_ID}\n"
                f"\tgcloud sql import sql {SQL_INSTANCE} {BUCKET_PATH}/{SQL_DUMP} --database={SQL_BASE} --user=postgres")


if __name__ == "__main__":
    set_env_vars()
    main()
