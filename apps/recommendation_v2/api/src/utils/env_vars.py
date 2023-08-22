import os
from enum import Enum
from utils.secrets import access_secret
import contextvars

GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
NUMBER_OF_RECOMMENDATIONS = 20
NUMBER_OF_PRESELECTED_OFFERS = 50 if not os.environ.get("CI") else 3
DEFAULT_SIMILAR_OFFER_MODEL = os.environ.get("DEFAULT_SIMILAR_OFFER_MODEL", "default")
API_LOCAL = os.environ.get("API_LOCAL", "False")
# SQL
# if not API_LOCAL:
SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE_PASSWORD = os.environ.get(
    "SQL_BASE_PASSWORD", access_secret(GCP_PROJECT, SQL_BASE_SECRET_ID)
)
SQL_PORT = os.environ.get("SQL_PORT")
SQL_HOST = os.environ.get("SQL_HOST")

# logger
cloud_trace_context = contextvars.ContextVar("cloud_trace_context", default="")
http_request_context = contextvars.ContextVar("http_request_context", default=dict({}))


class MixingFeatures(Enum):
    subcategory_id = "subcategory_id"
    search_group_name = "search_group_name"
    category = "category"
