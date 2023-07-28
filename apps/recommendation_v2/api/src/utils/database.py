from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import os

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "postgres")

engine = create_engine(
    # f"postgresql+psycopg2://postgres:postgres@testdb:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    f"postgresql+psycopg2://postgres:postgres@testdb/{DB_NAME}"
)

Base = declarative_base()


# from pcreco.utils.env_vars import (
#     SQL_BASE_USER,
#     SQL_BASE_PASSWORD,
#     SQL_BASE,
#     SQL_CONNECTION_NAME = "passculture-data-ehp:europe-west1:cloudsql-recommendation-dev-ew1"
# )

# query_string = dict({"host": "/cloudsql/{}".format(SQL_CONNECTION_NAME)})


# db_engine = None


# def load_engine():
#     global db_engine
#     db_engine = create_engine(
#         engine.url.URL(
#             drivername="postgres+psycopg2",
#             username=SQL_BASE_USER,
#             password=SQL_BASE_PASSWORD,
#             database=SQL_BASE,
#             query=query_string,
#         ),
#         pool_size=3,
#         max_overflow=15,
#         pool_timeout=30,
#         pool_recycle=1800,
#     )
