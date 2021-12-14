import os
import time
from typing import Any

from access_gcp_secrets import access_secret
from sqlalchemy import create_engine, engine, text
from loguru import logger

GCP_PROJECT = os.environ.get("GCP_PROJECT")

SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_BASE_SECRET_VERSION = os.environ.get("SQL_BASE_SECRET_VERSION")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")

SQL_BASE_PASSWORD = access_secret(
    GCP_PROJECT, SQL_BASE_SECRET_ID, SQL_BASE_SECRET_VERSION
)

AB_TESTING_TABLE = os.environ.get(
    "AB_TESTING_TABLE", "ab_testing"
)  # "ab_testing" for tests in circle ci
AB_TESTING_TABLE_EAC = os.environ.get("AB_TESTING_TABLE_EAC")
NUMBER_OF_RECOMMENDATIONS = 10
NUMBER_OF_PRESELECTED_OFFERS = 50 if not os.environ.get("CI") else 3

MODEL_REGION = os.environ.get("MODEL_REGION")
MODEL_NAME_A = os.environ.get("MODEL_NAME_A")
MODEL_NAME_B = os.environ.get("MODEL_NAME_B")
MODEL_NAME_C = os.environ.get("MODEL_NAME_C")

query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)


def create_pool():
    return create_engine(
        engine.url.URL(
            drivername="postgres+pg8000",
            username=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            query=query_string,
        ),
        pool_size=20,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )


def create_db_connection() -> Any:
    return create_pool().connect().execution_options(autocommit=True)


def log_duration(message, start):
    logger.info(f"{message}: {time.time() - start} seconds.")
