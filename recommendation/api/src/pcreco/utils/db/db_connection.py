from sqlalchemy import create_engine, engine, text
from typing import Any, Dict, List, Tuple

from pcreco.utils.env_vars import (
    SQL_BASE_USER,
    SQL_BASE_PASSWORD,
    SQL_BASE,
    SQL_CONNECTION_NAME,
)

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
    return __create_db_connection()


def __create_db_connection() -> Any:
    connection = create_pool().connect().execution_options(autocommit=True)
    yield connection
    connection.close()
