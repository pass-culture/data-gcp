import os

import pandas as pd
import psycopg2
import pytest
from numpy.testing import assert_array_equal
from sqlalchemy import create_engine

from recommendation import get_recommendations_for_user


TEST_DATABASE_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": os.getenv("DATA_GCP_TEST_POSTGRES_PORT"),
    "database": "postgres",
}


@pytest.fixture
def setup_database():
    """
    Fixture to set up the test postgres database with test data.
    """
    connection = psycopg2.connect(**TEST_DATABASE_CONFIG)
    cursor = connection.cursor()

    engine = create_engine(
        f'postgresql+psycopg2://postgres:postgres@127.0.0.1:{os.getenv("DATA_GCP_TEST_POSTGRES_PORT")}/postgres'
    )

    recommendable_offers = pd.DataFrame(
        {
            "id": [1, 2, 3],  # BIGINT,
            "venue_id": [11, 22, 33],
            "type": ["A", "B", "C"],
            "name": ["a", "b", "c"],
            "url": [None, None, "url"],
            "is_national": [True, True, True],
        }
    )
    recommendable_offers.to_sql("recommendable_offers", con=engine, if_exists="replace")

    non_recommendable_offers = pd.DataFrame(
        {"user_id": [111, 222, 333], "offer_id": [1, 2, 3]}
    )
    non_recommendable_offers.to_sql(
        "non_recommendable_offers", con=engine, if_exists="replace"
    )

    return connection, cursor


def test_user_recommendation_query(setup_database):
    """
    Test that the query used in api.recommendation is functionning
    """
    connection, cursor = setup_database

    user_recommendation = get_recommendations_for_user(111, 10, connection)

    assert_array_equal(
        user_recommendation,
        [{"id": 2, "type": "B", "url": None}, {"id": 3, "type": "C", "url": "url"}],
    )

    cursor.close()
    connection.close()
