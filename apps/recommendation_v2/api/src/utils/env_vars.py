import os
from enum import Enum
from utils.secrets import access_secret

GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-ehp")
NUMBER_OF_RECOMMENDATIONS = 20
NUMBER_OF_PRESELECTED_OFFERS = 50 if not os.environ.get("CI") else 3
DEFAULT_SIMILAR_OFFER_MODEL = os.environ.get("DEFAULT_SIMILAR_OFFER_MODEL", "default")
# SQL
SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE_PASSWORD = os.environ.get(
    "SQL_BASE_PASSWORD", access_secret(GCP_PROJECT, SQL_BASE_SECRET_ID)
)

class MixingFeatures(Enum):
    subcategory_id = "subcategory_id"
    search_group_name = "search_group_name"
    category = "category"
