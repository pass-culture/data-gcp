import os
from typing import Any
from unittest.mock import patch, Mock
import pytest

from pcreco.utils.health_check_queries import (
    does_materialized_view_exist,
    get_materialized_view_status,
)


@pytest.mark.parametrize(
    "materialized_view_name,expected_result",
    [("recommendable_offers_raw_mv", True)],
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


@patch("pcreco.utils.health_check_queries.does_materialized_view_exist")
def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
    does_materialized_view_exist_mock: Mock,
    setup_database: Any,
):
    # Given
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        does_materialized_view_exist_mock.return_value = True
        materialized_view_name = "materialized_view_name"
        connection_mock.return_value = setup_database
        # When
        result = get_materialized_view_status(materialized_view_name)

        # Then
        assert result["is_materialized_view_name_datasource_exists"] is True
