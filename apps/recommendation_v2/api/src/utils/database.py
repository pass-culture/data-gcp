from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import os

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "postgres")

engine = create_engine(
    f"postgresql+psycopg2://postgres:postgres@localhost:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
)

Base = declarative_base()
