import os
from typing import Any
from unittest.mock import patch, Mock

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pcreco.utils.health_check_queries import (
    does_materialized_view_exist,
    does_materialized_view_have_data,
    get_materialized_view_status,
)


DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME")


@pytest.fixture
def setup_database() -> Any:

    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
    connection = engine.connect().execution_options(autocommit=True)
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS recommendable_offers CASCADE;")
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;")
    engine.execute("DROP TABLE IF EXISTS temporary_table CASCADE;")
    engine.execute("DROP TABLE IF EXISTS empty_temporary_table CASCADE;")

    recommendable_offers = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],  # BIGINT,
            "venue_id": [11, 22, 33, 44, 55],
            "type": ["A", "B", "C", "D", "E"],
            "name": ["a", "b", "c", "d", "e"],
            "url": [None, None, "url", "url", None],
            "is_national": [True, False, True, False, True],
        }
    )
    recommendable_offers.to_sql("temporary_table", con=engine)
    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers AS SELECT * FROM temporary_table WITH DATA;"
    )

    pd.DataFrame({}).to_sql("empty_temporary_table", con=engine)
    engine.execute(
        "CREATE MATERIALIZED VIEW non_recommendable_offers AS SELECT * FROM empty_temporary_table;"
    )

    yield connection

    engine.execute("DROP MATERIALIZED VIEW IF EXISTS recommendable_offers CASCADE;")
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;")
    engine.execute("DROP TABLE IF EXISTS temporary_table CASCADE;")
    engine.execute("DROP TABLE IF EXISTS empty_temporary_table CASCADE;")
    connection.close()


@pytest.mark.parametrize(
    "materialized_view_name,expected_result",
    [("recommendable_offers", True), ("iris_venues_mv", False)],
)
def test_does_view_exist(
    setup_database: Any, materialized_view_name: str, expected_result: bool
):
    # Given
    connection = setup_database

    # When
    result = does_materialized_view_exist(connection, materialized_view_name)

    # Then
    assert result is expected_result


@pytest.mark.parametrize(
    "materialized_view_name,expected_result",
    [
        ("recommendable_offers", True),
        ("non_recommendable_offers", False),
        ("iris_venues_mv", False),
    ],
)
def test_does_view_have_data(
    setup_database: Any, materialized_view_name: str, expected_result: bool
):
    # Given
    connection = setup_database

    # When
    result = does_materialized_view_have_data(connection, materialized_view_name)

    # Then
    assert result is expected_result


@patch("src.pcreco.utils.health_check_queries.does_materialized_view_have_data")
@patch("src.pcreco.utils.health_check_queries.does_materialized_view_exist")
@patch("src.pcreco.utils.db.db_connection.create_db_connection")
def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
    connection_mock: Mock,
    does_materialized_view_exist_mock: Mock,
    does_materialized_view_have_data_mock: Mock,
):
    # Given
    does_materialized_view_exist_mock.return_value = True
    does_materialized_view_have_data_mock.return_value = False
    materialized_view_name = "materialized_view_name"

    # When
    result = get_materialized_view_status(materialized_view_name)

    # Then
    assert result["is_materialized_view_name_datasource_exists"] is True
    assert result["is_materialized_view_name_ok"] is False
