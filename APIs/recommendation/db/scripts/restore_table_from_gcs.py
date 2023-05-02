import logging
import sys

from set_env import set_env_vars

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_REGION = "europe-west1"
BUCKET_PATH = "gs://pass-culture-data"
SQL_DUMP = "dump_staging_for_recommendations_09_11_20.gz"
SQL_INSTANCE = "pcdata-poc-csql-recommendation"
SQL_BASE = "pcdata-poc-csql-recommendation"


def main():
    logger.info(
        f"\n\tRun this commands in cloudshell (or your terminal if you have gcloud SDK installed) "
        f"to restore data in CloudSQL from {BUCKET_PATH}/{SQL_DUMP}:\n\n"
        f"\tgcloud sql connect {SQL_INSTANCE}\n"
        f"\t\connect {SQL_BASE}\n"
        + "\n".join(
            [
                f"\tDROP TABLE IF EXISTS public.{table};"
                for table in [
                    "booking",
                    "offer",
                    "iris_venues_at_radius",
                    "stock",
                    "mediation",
                    "venue",
                    "offerer",
                ]
            ]
        )
        + f"\n\n\tThen in an other terminal:\n\n"
        f"\tgcloud config set project {GCP_PROJECT_ID}\n"
        f"\tgcloud sql import sql {SQL_INSTANCE} {BUCKET_PATH}/{SQL_DUMP} --database={SQL_BASE} --user=postgres"
    )


if __name__ == "__main__":
    set_env_vars()
    main()
