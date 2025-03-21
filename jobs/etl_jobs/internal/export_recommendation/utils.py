import logging
import os
import time

import psycopg2
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from psycopg2.extensions import connection

logger = logging.getLogger(__name__)


ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
ENV_LONG_NAME = {
    "prod": "production",
    "stg": "stg",
    "dev": "dev",
}[ENV_SHORT_NAME]
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_ML_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"
BIGQUERY_ML_RETRIEVAL_DATASET = f"ml_retrieval_{ENV_SHORT_NAME}"
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_SEED_DATASET = f"seed_{ENV_SHORT_NAME}"
RECOMMENDATION_SQL_INSTANCE = f"cloudsql-recommendation-{ENV_LONG_NAME}-ew1"
REGION = "europe-west1"
# Constants for processing
MAX_RETRIES = 5


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def get_db_connection() -> connection:
    """Create a database connection with retries."""
    database_url = access_secret_data(
        PROJECT_NAME,
        f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
    )

    retry_count = 0
    conn = None

    while retry_count < MAX_RETRIES and conn is None:
        try:
            conn = psycopg2.connect(database_url)
            conn.autocommit = False
            return conn
        except Exception as e:
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                logger.error(
                    f"Failed to connect to database after {MAX_RETRIES} attempts: {str(e)}"
                )
                raise

            wait_time = min(30, 5 * retry_count)
            logger.warning(
                f"Database connection failed (attempt {retry_count}/{MAX_RETRIES}). Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)
