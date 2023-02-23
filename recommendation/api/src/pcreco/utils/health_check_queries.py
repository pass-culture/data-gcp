import logging
import sys
from typing import Any

from pcreco.utils.db.db_connection import get_session

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def does_materialized_view_exist(connection: Any, materialized_view_name: str) -> bool:
    query = f"""SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{materialized_view_name}');"""
    return connection.execute(query).scalar()


def get_materialized_view_status(materialized_view_name: str) -> dict:
    connection = get_session()

    materialized_view_status = {
        f"is_{materialized_view_name}_datasource_exists": does_materialized_view_exist(
            connection, materialized_view_name
        )
    }
    return materialized_view_status
