from sqlalchemy import create_engine, engine, text
from sqlalchemy.pool import NullPool
from typing import Any, Dict, List, Tuple
from flask import current_app, g

from pcreco.utils.env_vars import (
    SQL_BASE_USER,
    SQL_BASE_PASSWORD,
    SQL_BASE,
    SQL_CONNECTION_NAME,
)

query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)

db_engine = None


def load_engine():
    global db_engine
    db_engine = create_engine(
        engine.url.URL(
            drivername="postgres+pg8000",
            username=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            query=query_string,
        ),
        pool_size=3,
        max_overflow=15,
        pool_timeout=30,
        pool_recycle=1800,
    )


def create_connection():
    if db_engine is None:
        load_engine()
    connection = db_engine.connect()
    g.connection = connection
    g.engine = db_engine


def close_connection():
    try:
        g.connection.close()
    except:
        pass
