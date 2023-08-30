from typing import Any
import pytest
from sqlalchemy import text


def does_materialized_view_exist(connection: Any, materialized_view_name: str) -> bool:
    query = f"""SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{materialized_view_name}');"""
    return connection.execute(text(query)).scalar()


@pytest.mark.parametrize(
    "materialized_view_name,expected_result",
    [("recommendable_offers_per_iris_shape_mv", True)],
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
