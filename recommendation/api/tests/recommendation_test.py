import os
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import pandas as pd
import psycopg2
import pytest
from numpy.testing import assert_array_equal
from sqlalchemy import create_engine

from recommendation import (
    get_recommendations_for_user,
    get_scored_recommendation_for_user,
    order_offers_by_score_and_diversify_types,
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


@pytest.fixture
def setup_database() -> Tuple[Any, Any]:
    connection = psycopg2.connect(**TEST_DATABASE_CONFIG)
    cursor = connection.cursor()

    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )

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
    recommendable_offers.to_sql("recommendable_offers", con=engine, if_exists="replace")

    non_recommendable_offers = pd.DataFrame({"user_id": [111], "offer_id": [1]})
    non_recommendable_offers.to_sql(
        "non_recommendable_offers", con=engine, if_exists="replace"
    )

    iris_venues = pd.DataFrame({"irisId": [1, 1, 1, 2], "venueId": [11, 22, 33, 44]})
    iris_venues.to_sql("iris_venues", con=engine, if_exists="replace")

    return connection, cursor


def test_get_recommendation_for_user(setup_database: Tuple[Any, Any]):
    # Given
    connection, cursor = setup_database

    # When
    user_id = 111
    user_iris_id = 1
    user_recommendation = get_recommendations_for_user(
        user_id, user_iris_id, connection
    )

    # Then
    assert_array_equal(
        sorted(user_recommendation, key=lambda k: k["id"]),
        [
            {"id": 2, "type": "B", "url": None},
            {"id": 3, "type": "C", "url": "url"},
            {"id": 5, "type": "E", "url": None},
        ],
    )

    cursor.close()
    connection.close()


def test_get_recommendation_for_user_with_no_iris(setup_database: Tuple[Any, Any]):
    # Given
    connection, cursor = setup_database

    # When
    user_id = 111
    user_iris_id = None
    user_recommendation = get_recommendations_for_user(
        user_id, user_iris_id, connection
    )

    # Then
    assert_array_equal(
        sorted(user_recommendation, key=lambda k: k["id"]),
        [{"id": 3, "type": "C", "url": "url"}, {"id": 5, "type": "E", "url": None}],
    )

    cursor.close()
    connection.close()


@pytest.mark.parametrize(
    ["offers", "output"],
    [
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 1},
                {"id": 3, "url": "https://url.com", "type": "B", "score": 10},
                {"id": 4, "url": None, "type": "B", "score": 10},
            ],
            [2, 3, 4, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 1},
                {"id": 3, "url": None, "type": "B", "score": 10},
                {"id": 4, "url": None, "type": "B", "score": 10},
            ],
            [4, 2, 3, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 2},
                {"id": 3, "url": None, "type": "A", "score": 10},
                {"id": 4, "url": None, "type": "A", "score": 11},
            ],
            [4, 3, 2, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "score": 1},
                {"id": 2, "url": None, "type": "A", "score": 2},
                {"id": 3, "url": "test", "type": "A", "score": 10},
                {"id": 4, "url": "test", "type": "A", "score": 11},
            ],
            [4, 2, 3, 1],
        ),
    ],
)
def test_order_offers_by_score_and_diversify_types(
    offers: List[Dict[str, Any]], output: List[int]
):
    assert_array_equal(output, order_offers_by_score_and_diversify_types(offers))


@patch("recommendation.predict_score")
def test_get_scored_recommendation_for_user(predict_score_mock):
    # Given
    predict_score_mock.return_value = [1, 2, 3]
    user_recommendation = [
        {"id": 1, "url": "url1", "type": "type1"},
        {"id": 2, "url": "url2", "type": "type2"},
        {"id": 3, "url": "url3", "type": "type3"},
    ]
    model_name = "model"
    version = "v"

    # When
    scored_recommendation_for_user = get_scored_recommendation_for_user(
        user_recommendation, model_name, version
    )

    # Then
    assert scored_recommendation_for_user == [
        {"id": 1, "url": "url1", "type": "type1", "score": 1},
        {"id": 2, "url": "url2", "type": "type2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "score": 3},
    ]
