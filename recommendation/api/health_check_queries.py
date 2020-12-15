import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, session


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()

SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_PASSWORD = os.environ.get("SQL_BASE_PASSWORD")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE = os.environ.get("SQL_BASE")

DATABASE_URL = f"postgresql+psycopg2://{SQL_BASE_USER}:{SQL_BASE_PASSWORD}@{SQL_CONNECTION_NAME}/{SQL_BASE}"
health_check_engine = create_engine(
    DATABASE_URL, connect_args={"options": "-c statement_timeout=30000"}
)
health_check_session = sessionmaker(bind=health_check_engine)()


def does_materialized_view_exist(
    health_check_session: session.Session, materialized_view_name: str
) -> bool:
    query = f"""SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{materialized_view_name}');"""
    is_data_present = health_check_session.execute(query).scalar()
    return is_data_present


def does_materialized_view_have_data(
    health_check_session: session.Session, materialized_view_name: str
) -> bool:
    is_materialized_view_with_data = False
    if does_materialized_view_exist(health_check_session, materialized_view_name):
        query = f"""SELECT EXISTS(SELECT * FROM { materialized_view_name} limit 1);"""
        is_materialized_view_with_data = health_check_session.execute(query).scalar()
    return is_materialized_view_with_data


def get_materialized_view_status(materialized_view_name: str) -> dict:
    materialized_view_status = {
        f"is_{materialized_view_name}_datasource_exists": does_materialized_view_exist(
            health_check_session, materialized_view_name
        ),
        f"is_{materialized_view_name}_ok": does_materialized_view_have_data(
            health_check_session, materialized_view_name
        ),
    }
    health_check_session.close()
    return materialized_view_status
