import os
import time
from pcreco.utils.secrets.access_gcp_secrets import access_secret
from loguru import logger

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
MODEL_REGION = os.environ.get("MODEL_REGION")
DATA_BUCKET = os.environ.get("DATA_BUCKET")
QPI_FOLDER = os.environ.get("QPI_FOLDER", "src/tests/qpi_export_test")
MODEL_END_POINT = f"https://{MODEL_REGION}-ml.googleapis.com"
# SQL
SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE_PASSWORD = os.environ.get(
    "SQL_BASE_PASSWORD", access_secret(GCP_PROJECT, SQL_BASE_SECRET_ID)
)
# Vertex ai attributes
DEFAULT_RECO_MODEL = os.environ.get("DEFAULT_RECO_MODEL", "default")
DEFAULT_SIMILAR_OFFER_MODEL = os.environ.get("DEFAULT_SIMILAR_OFFER_MODEL", "default")


# Attributes on API output and recommendation
NUMBER_OF_RECOMMENDATIONS = 20
SHUFFLE_RECOMMENDATION = False
MIXING_RECOMMENDATION = True
MIXING_FEATURE = "subcategory_id"
MIXING_FEATURE_LIST = ["subcategory_id", "search_group_name", "category"]
NUMBER_OF_PRESELECTED_OFFERS = 200 if not os.environ.get("CI") else 3
RECOMMENDABLE_OFFER_LIMIT = 5000
COLD_START_RECOMMENDABLE_OFFER_LIMIT = 500
DEFAULT_RECO_RADIUS = ["0_25KM", "25_50KM", "50_100KM"]

RECOMMENDABLE_OFFER_TABLE_PREFIX = "recommendable_offers_per_iris_shape"


def log_duration(message, start):
    logger.info(f"{message}: {time.time() - start} seconds.")
