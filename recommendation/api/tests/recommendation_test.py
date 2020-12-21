import datetime
import os
from typing import Any, Dict, List, Tuple
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import pytz
from numpy.testing import assert_array_equal
from sqlalchemy import create_engine

from recommendation import (
    get_final_recommendations,
    get_intermediate_recommendations_for_user,
    get_scored_recommendation_for_user,
    order_offers_by_score_and_diversify_types,
    save_recommendation,
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
def app_config() -> Dict[str, Any]:
    return {
        "AB_TESTING_TABLE": "ab_testing_20201207",
        "NUMBER_OF_RECOMMENDATIONS": 10,
        "MODEL_NAME": "model_name",
        "MODEL_VERSION": "model_version",
    }


@pytest.fixture
def setup_database(app_config: Dict[str, Any]) -> Any:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
    connection = engine.connect().execution_options(autocommit=True)
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

    non_recommendable_offers = pd.DataFrame({"user_id": [111, 112], "offer_id": [1, 3]})
    non_recommendable_offers.to_sql(
        "non_recommendable_offers", con=engine, if_exists="replace"
    )

    iris_venues_mv = pd.DataFrame(
        {"iris_id": [1, 1, 1, 2], "venue_id": [11, 22, 33, 44]}
    )
    iris_venues_mv.to_sql("iris_venues_mv", con=engine, if_exists="replace")

    ab_testing = pd.DataFrame({"userid": [111, 112], "groupid": ["A", "B"]})
    ab_testing.to_sql(app_config["AB_TESTING_TABLE"], con=engine, if_exists="replace")

    past_recommended_offers = pd.DataFrame(
        {"userid": [1], "offerid": [1], "date": [datetime.datetime.now(pytz.utc)]}
    )
    past_recommended_offers.to_sql(
        "past_recommended_offers", con=engine, if_exists="replace"
    )

    yield connection

    engine.execute("DROP TABLE IF EXISTS recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS non_recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS iris_venues;")
    engine.execute(f"DROP TABLE IF EXISTS {app_config['AB_TESTING_TABLE']} ;")
    engine.execute(f"DROP TABLE IF EXISTS past_recommended_offers ;")
    connection.close()


@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_iris_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("recommendation.create_db_connection")
def test_get_final_recommendation_for_group_a(
    connection_mock: Mock,
    save_recommendation_mock: Mock,
    get_iris_from_coordinates_mock: Mock,
    get_scored_recommendation_for_user_mock: Mock,
    get_intermediate_recommendations_for_user_mock: Mock,
    setup_database: Any,
    app_config: Dict[str, Any],
):
    # Given
    connection_mock.return_value = setup_database
    user_id = 111
    get_intermediate_recommendations_for_user_mock.return_value = [
        {"id": 2, "url": "url2", "type": "type2"},
        {"id": 3, "url": "url3", "type": "type3"},
    ]
    get_scored_recommendation_for_user_mock.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "score": 3},
    ]
    get_iris_from_coordinates_mock.return_value = 1

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )

    # Then
    assert recommendations == [3, 2]
    save_recommendation_mock.assert_called_once()


@patch("recommendation.save_recommendation")
@patch("recommendation.create_db_connection")
def test_get_final_recommendation_for_group_b(
    connection_mock: Mock,
    save_recommendation_mock: Mock,
    setup_database: Any,
    app_config: Dict[str, Any],
):
    # Given
    connection_mock.return_value = setup_database
    user_id = 112

    # When
    recommendations = get_final_recommendations(user_id, None, None, app_config)

    # Then
    assert recommendations == []
    save_recommendation_mock.assert_not_called()


@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_iris_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("recommendation.create_db_connection")
def test_get_final_recommendation_for_new_user(
    connection_mock: Mock,
    save_recommendation_mock: Mock,
    get_iris_from_coordinates_mock: Mock,
    get_scored_recommendation_for_user_mock: Mock,
    get_intermediate_recommendations_for_user_mock: Mock,
    setup_database: Any,
    app_config: Dict[str, Any],
):
    # Given
    connection_mock.return_value = setup_database
    user_id = 113
    get_intermediate_recommendations_for_user_mock.return_value = []
    get_scored_recommendation_for_user_mock.return_value = []
    get_iris_from_coordinates_mock.return_value = 1

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )
    save_recommendation_mock.assert_not_called()

    # Then
    assert recommendations == []


def test_get_intermediate_recommendation_for_user(setup_database: Any):
    # Given
    connection = setup_database

    # When
    user_id = 111
    user_iris_id = 1
    user_recommendation = get_intermediate_recommendations_for_user(
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


def test_get_intermediate_recommendation_for_user_with_no_iris(setup_database: Any):
    # Given
    connection = setup_database

    # When
    user_id = 222
    user_iris_id = None
    user_recommendation = get_intermediate_recommendations_for_user(
        user_id, user_iris_id, connection
    )

    # Then
    assert_array_equal(
        sorted(user_recommendation, key=lambda k: k["id"]),
        [
            {"id": 1, "type": "A", "url": None},
            {"id": 3, "type": "C", "url": "url"},
            {"id": 4, "type": "D", "url": "url"},
            {"id": 5, "type": "E", "url": None},
        ],
    )


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
def test_get_scored_recommendation_for_user(
    predict_score_mock: Mock,
    app_config: Dict[str, Any],
):
    # Given
    predict_score_mock.return_value = [1, 2, 3]
    user_recommendation = [
        {"id": 1, "url": "url1", "type": "type1"},
        {"id": 2, "url": "url2", "type": "type2"},
        {"id": 3, "url": "url3", "type": "type3"},
    ]

    # When
    scored_recommendation_for_user = get_scored_recommendation_for_user(
        user_recommendation, app_config["MODEL_NAME"], app_config["MODEL_VERSION"]
    )

    # Then
    assert scored_recommendation_for_user == [
        {"id": 1, "url": "url1", "type": "type1", "score": 1},
        {"id": 2, "url": "url2", "type": "type2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "score": 3},
    ]


def test_save_recommendation(setup_database: Tuple[Any, Any]):
    # Given
    user_id = 1
    recommendations = [2, 3, 4]
    connection = setup_database

    # When
    save_recommendation(user_id, recommendations, connection)

    # Then
    for offer_id in recommendations:
        query_result = connection.execute(
            f"SELECT * FROM public.past_recommended_offers where userid = {user_id} and offerId = {offer_id}"
        ).fetchall()
        assert len(query_result) == 1
