import os
from sqlalchemy import create_engine, engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from huggy.utils.env_vars import (
    SQL_BASE_USER,
    SQL_BASE_PASSWORD,
    SQL_BASE,
    SQL_HOST,
    SQL_PORT,
    API_LOCAL,
)

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT", 5432)

query = {}

if API_LOCAL is True:
    DB_NAME = "postgres"

    bind_engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@localhost:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )

else:
    bind_engine = create_engine(
        engine.url.URL(
            drivername="postgresql+psycopg2",
            username=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            host=SQL_HOST,
            port=SQL_PORT,
            query=query,
        ),
        pool_size=3,
        max_overflow=15,
        pool_timeout=30,
        pool_recycle=1800,
        client_encoding="utf8",
    )
Base = declarative_base()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=bind_engine)
