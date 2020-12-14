import os
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from health_check_queries import (
    does_materialize_view_exist,
    does_materialized_view_contain_data,
    does_view_have_data,
    get_materialized_view_status,
    is_materialized_view_queryable,
)


DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME")


@pytest.fixture
def setup_database() -> Any:

    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
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
    session = sessionmaker(bind=engine)()

    yield session

    engine.execute("DROP MATERIALIZED VIEW IF EXISTS recommendable_offers CASCADE;")
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;")
    engine.execute("DROP TABLE IF EXISTS temporary_table CASCADE;")
    engine.execute("DROP TABLE IF EXISTS empty_temporary_table CASCADE;")
    session.close()


class DoesViewExistTest:
    def test_should_return_false_if_materialized_view_does_not_exist(
        self, setup_database
    ):
        # Given
        session = setup_database

        # When
        result = does_materialize_view_exist(session, "iris_venues_mv")

        # Then
        assert result is False

    def test_should_return_true_if_materialized_view_exist(self, setup_database):
        # Given
        session = setup_database

        # When
        result = does_materialize_view_exist(session, "recommendable_offers")

        # Then
        assert result is True


class IsMaterializedViewQueryableTest:
    @patch("health_check_queries.does_materialize_view_exist")
    def test_should_close_session_if_query_did_not_end_on_an_exception(
        self, does_materialize_view_exist_mock, setup_database
    ):
        # Given
        does_materialize_view_exist_mock.return_value = True
        session = MagicMock()

        # When
        result = is_materialized_view_queryable(session, "materialized_view_name")

        # Then
        assert result is True
        session.close.assert_called_once()

    @patch("health_check_queries.does_materialize_view_exist")
    @patch("health_check_queries.logger")
    def test_should_log_error_and_close_session_when_operational_error_is_raised(
        self, logger_mock, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.side_effect = OperationalError("", "", "")
        session = MagicMock()

        # When
        result = is_materialized_view_queryable(session, "materialized_view_name")

        # Then
        assert result is False
        session.close.assert_called_once()
        logger_mock.error.assert_called_once()

    @patch("health_check_queries.does_materialize_view_exist")
    @patch("health_check_queries.logger")
    def test_should_log_error_and_close_session_when_an_sql_alchemy_error_is_raised(
        self, logger_mock, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.side_effect = SQLAlchemyError("", "", "")
        session = MagicMock()

        # When
        result = is_materialized_view_queryable(session, "materialized_view_name")

        # Then
        assert result is False
        session.close.assert_called_once()
        logger_mock.error.assert_called_once()

    @patch("health_check_queries.does_materialize_view_exist")
    @patch("health_check_queries.logger")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, logger_mock, does_materialize_view_exist_mock
    ):
        # Given
        does_materialize_view_exist_mock.side_effect = Exception
        session = MagicMock()

        # When
        with pytest.raises(Exception):
            is_materialized_view_queryable(session, "materialized_view_name")

        # Then
        session.close.assert_not_called()
        logger_mock.error.assert_not_called()


class DoesViewHaveDataTest:
    def test_should_return_false_if_view_exists_but_is_empty(self, setup_database):
        # Given
        session = setup_database

        # When
        result = does_view_have_data(session, "non_recommendable_offers")

        session.close()
        # Then
        assert result is False

    def test_should_return_true_if_view_has_entries(self, setup_database):
        # Given
        session = setup_database

        # When
        result = does_view_have_data(session, "recommendable_offers")
        session.close()

        # Then
        assert result is True


class DoesMaterializedViewContainDataTest:
    @patch("health_check_queries.is_materialized_view_queryable")
    @patch("health_check_queries.does_view_have_data")
    def test_should_return_false_when_the_view_is_not_found(
        self, does_view_have_data_mock, is_materialized_view_queryable_mock
    ):
        # Given
        is_materialized_view_queryable_mock.return_value = False
        session = MagicMock()

        # When
        result = does_materialized_view_contain_data(session, "materialized_view_name")

        # Then
        assert result is False
        is_materialized_view_queryable_mock.assert_called_once_with(
            session, "materialized_view_name"
        )
        does_view_have_data_mock.assert_not_called()

    @patch("health_check_queries.is_materialized_view_queryable")
    @patch("health_check_queries.does_view_have_data")
    def test_should_return_false_if_view_is_empty(
        self, does_view_have_data_mock, is_materialized_view_queryable_mock
    ):
        # Given
        is_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = False
        session = MagicMock()

        # When
        result = does_materialized_view_contain_data(session, "materialized_view_name")

        # Then
        assert result is False
        session.close.assert_called_once()
        is_materialized_view_queryable_mock.assert_called_once_with(
            session, "materialized_view_name"
        )
        does_view_have_data_mock.assert_called_once_with(
            session, "materialized_view_name"
        )

    @patch("health_check_queries.is_materialized_view_queryable")
    @patch("health_check_queries.does_view_have_data")
    def test_should_return_true_if_view_has_data(
        self, does_view_have_data_mock, is_materialized_view_queryable_mock
    ):
        # Given
        is_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.return_value = True
        session = MagicMock()

        # When
        result = does_materialized_view_contain_data(session, "materialized_view_name")

        # Then
        assert result is True
        session.close.assert_called_once()
        is_materialized_view_queryable_mock.assert_called_once_with(
            session, "materialized_view_name"
        )
        does_view_have_data_mock.assert_called_once_with(
            session, "materialized_view_name"
        )

    @patch("health_check_queries.is_materialized_view_queryable")
    @patch("health_check_queries.does_view_have_data")
    def test_should_return_false_when_there_is_an_sql_alchemy_error_on_query(
        self, does_view_have_data_mock, is_materialized_view_queryable_mock
    ):
        # Given
        is_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = SQLAlchemyError
        session = MagicMock()

        # When
        result = does_materialized_view_contain_data(session, "materialized_view_name")

        # Then
        assert result is False
        session.close.assert_called_once()

    @patch("health_check_queries.is_materialized_view_queryable")
    @patch("health_check_queries.does_view_have_data")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self, does_view_have_data_mock, is_materialized_view_queryable_mock
    ):
        # Given
        is_materialized_view_queryable_mock.return_value = True
        does_view_have_data_mock.side_effect = Exception
        session = MagicMock()

        # When
        with pytest.raises(Exception):
            result = does_materialized_view_contain_data(
                session, "materialized_view_name"
            )
            # Then
            assert result is None

        session.close.assert_not_called()


class GetMaterializedViewStatusTest:
    @patch("health_check_queries.is_materialized_view_queryable")
    @patch("health_check_queries.does_materialized_view_contain_data")
    def test_should_raise_exception_when_it_does_not_come_from_sql_alchemy(
        self,
        does_materialized_view_contain_data_mock,
        is_materialized_view_queryable_mock,
    ):
        # Given
        is_materialized_view_queryable_mock.return_value = True
        does_materialized_view_contain_data_mock.return_value = False
        materialized_view_name = "materialized_view_name"

        # When
        result = get_materialized_view_status(materialized_view_name)

        # Then
        result == {
            f"is_materialized_view_name_datasource_exists": True,
            f"is_materialized_view_name_ok": False,
        }
