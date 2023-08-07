from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import os
from utils.env_vars import (
    SQL_BASE_USER,
    SQL_BASE_PASSWORD,
    SQL_BASE,
    SQL_CONNECTION_NAME
)

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "postgres")


engine = create_engine(
    # f"postgresql+psycopg2://postgres:postgres@testdb:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    # f"postgresql+psycopg2://postgres:postgres@testdb/{DB_NAME}"
    f"postgresql+psycopg2://{SQL_BASE_USER}:{SQL_BASE_PASSWORD}@/{SQL_BASE}?host=/cloudsql/{SQL_CONNECTION_NAME}"
)

Base = declarative_base()
