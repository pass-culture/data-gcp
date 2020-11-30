import os
from typing import Any, Tuple
import pytest
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

from geolocalisation import get_iris_from_coordinates

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME")

TEST_DATABASE_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": DATA_GCP_TEST_POSTGRES_PORT,
    "database": DB_NAME,
}


@pytest.fixture
def setup_database() -> Tuple[Any, Any]:
    connection = psycopg2.connect(**TEST_DATABASE_CONFIG)
    cursor = connection.cursor()

    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@localhost:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )

    iris_france = pd.read_csv("tests/iris_france_tests.csv")
    iris_france.to_sql("iris_france", con=engine, if_exists="replace")

    sql = """ALTER TABLE public.iris_france
            ALTER COLUMN shape TYPE Geometry(GEOMETRY, 4326)
            USING ST_SetSRID(shape::Geometry, 4326);
        """

    cursor.execute(sql)
    cursor.close()

    return connection, cursor


def test_get_iris_from_coordinates(setup_database: Tuple[Any, Any]):
    # Given
    connection, cursor = setup_database

    # When
    iris_id = get_iris_from_coordinates(2.331289, 48.830719, connection)

    # Then
    assert iris_id == 45327

    cursor.close()
    connection.close()
