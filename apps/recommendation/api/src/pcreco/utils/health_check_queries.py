import logging
import sys
from typing import Any

from pcreco.utils.db.db_connection import get_session

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def does_materialized_view_exist(connection: Any, materialized_view_name: str) -> bool:
    query = f"""SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{materialized_view_name}');"""
    return connection.execute(query).scalar()


def get_available_materialized_view(materialized_view_name: str) -> bool:
    connection = get_session()
    for suffix in ["_mv", "_mv_tmp", "_mv_old"]:
        table_name = f"{materialized_view_name}{suffix}"
        result = connection.execute(
            f"""SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{table_name}');"""
        ).scalar()
        if result:
            return table_name
    return materialized_view_name


def get_materialized_view_status(materialized_view_name: str) -> dict:
    connection = get_session()

    materialized_view_status = {
        f"is_{materialized_view_name}_datasource_exists": does_materialized_view_exist(
            connection, materialized_view_name
        )
    }
    return materialized_view_status
