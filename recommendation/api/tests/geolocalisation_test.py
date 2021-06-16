import os
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import create_engine
from unittest.mock import Mock, patch

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
def setup_database() -> Any:

    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@localhost:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
    connection = engine.connect().execution_options(autocommit=True)

    iris_france = pd.read_csv("tests/iris_france_tests.csv")
    iris_france.to_sql("iris_france", con=engine, if_exists="replace", index=False)

    sql = """ALTER TABLE public.iris_france
            ALTER COLUMN shape TYPE Geometry(GEOMETRY, 4326)
            USING ST_SetSRID(shape::Geometry, 4326);
        """

    connection.execute(sql)

    yield connection

    engine.execute("DROP TABLE IF EXISTS iris_france;")
    connection.close()


@patch("geolocalisation.create_db_connection")
def test_get_iris_from_coordinates(mock_connection: Mock, setup_database: Any):
    # Given
    mock_connection.return_value = setup_database

    # When
    longitude = 2.331289
    latitude = 48.830719
    iris_id = get_iris_from_coordinates(longitude, latitude)

    # Then
    assert iris_id == 45327


@patch("geolocalisation.create_db_connection")
def test_get_iris_from_coordinates_without_coordinates(
    mock_connection: Mock, setup_database: Any
):
    # Given
    mock_connection.return_value = setup_database

    # When
    longitude = None
    latitude = None
    iris_id = get_iris_from_coordinates(longitude, latitude)

    # Then
    assert iris_id is None


@patch("geolocalisation.create_db_connection")
def test_get_iris_from_coordinates_not_in_france(
    mock_connection: Mock, setup_database: Any
):
    # Given
    mock_connection.return_value = setup_database

    # When
    longitude = -122.1639346
    latitude = 37.4449422
    iris_id = get_iris_from_coordinates(longitude, latitude)

    # Then
    assert iris_id is None
