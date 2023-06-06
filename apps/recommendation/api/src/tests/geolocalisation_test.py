import os
from typing import Any, List
import pytest
from unittest.mock import patch


from pcreco.utils.geolocalisation import (
    get_iris_from_coordinates,
)

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME")

TEST_DATABASE_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": DATA_GCP_TEST_POSTGRES_PORT,
    "database": DB_NAME,
}


def test_get_iris_from_coordinates(setup_database: Any):
    # Given
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        connection_mock.return_value = setup_database

        # When
        longitude = 2.331289
        latitude = 48.830719
        iris_id = get_iris_from_coordinates(longitude, latitude)

        # Then
        assert iris_id == 45327


def test_get_iris_from_coordinates_without_coordinates(setup_database: Any):
    # Given
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        connection_mock.return_value = setup_database

        # When
        longitude = None
        latitude = None
        iris_id = get_iris_from_coordinates(longitude, latitude)

        # Then
        assert iris_id is None


def test_get_iris_from_coordinates_not_in_france(setup_database: Any):
    # Given
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        connection_mock.return_value = setup_database

        # When
        longitude = -122.1639346
        latitude = 37.4449422
        iris_id = get_iris_from_coordinates(longitude, latitude)

        # Then
        assert iris_id is None
