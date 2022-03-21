import logging
import os
import sys
from typing import Any

from sqlalchemy import create_engine, engine
from access_gcp_secrets import access_secret


GCP_PROJECT = os.environ.get("GCP_PROJECT")

SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_BASE_SECRET_VERSION = os.environ.get("SQL_BASE_SECRET_VERSION")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")


SQL_BASE_PASSWORD = access_secret(
    GCP_PROJECT, SQL_BASE_SECRET_ID, SQL_BASE_SECRET_VERSION
)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)
engine = create_engine(
    engine.url.URL(
        drivername="postgresql+pg8000",
        username=SQL_BASE_USER,
        password=SQL_BASE_PASSWORD,
        database=SQL_BASE,
        query=query_string,
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800,
)


def create_db_connection() -> Any:
    return engine.connect()


def does_materialized_view_exist(connection: Any, materialized_view_name: str) -> bool:
    query = f"""SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{materialized_view_name}');"""
    is_data_present = connection.execute(query).scalar()
    return is_data_present


def does_materialized_view_have_data(
    connection: Any, materialized_view_name: str
) -> bool:
    is_materialized_view_with_data = False
    if does_materialized_view_exist(connection, materialized_view_name):
        query = f"""SELECT EXISTS(SELECT * FROM { materialized_view_name} limit 1);"""
        is_materialized_view_with_data = connection.execute(query).scalar()
    return is_materialized_view_with_data


def get_materialized_view_status(materialized_view_name: str) -> dict:
    connection = create_db_connection()

    materialized_view_status = {
        f"is_{materialized_view_name}_datasource_exists": does_materialized_view_exist(
            connection, materialized_view_name
        ),
        f"is_{materialized_view_name}_ok": does_materialized_view_have_data(
            connection, materialized_view_name
        ),
    }
    connection.close()
    return materialized_view_status
