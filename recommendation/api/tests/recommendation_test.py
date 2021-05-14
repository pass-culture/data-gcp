from typing import Any, Dict, List, Tuple
from unittest.mock import Mock, patch

from numpy.testing import assert_array_equal
import pytest

from recommendation import (
    get_final_recommendations,
    get_intermediate_recommendations_for_user,
    get_scored_recommendation_for_user,
    order_offers_by_score_and_diversify_types,
    save_recommendation,
)


@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_accessible_departements_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("recommendation.create_db_connection")
def test_get_final_recommendation_for_group_a(
    connection_mock: Mock,
    save_recommendation_mock: Mock,
    get_accessible_departements_from_coordinates: Mock,
    get_scored_recommendation_for_user_mock: Mock,
    get_intermediate_recommendations_for_user_mock: Mock,
    setup_database: Any,
    app_config: Dict[str, Any],
):
    # Given
    connection_mock.return_value = setup_database
    user_id = 111
    get_intermediate_recommendations_for_user_mock.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2"},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3"},
    ]
    get_scored_recommendation_for_user_mock.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3", "score": 3},
    ]
    get_accessible_departements_from_coordinates.return_value = ["93"]

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )

    # Then
    assert recommendations == [3, 2]
    save_recommendation_mock.assert_called_once()


@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_accessible_departements_from_coordinates")
@patch("recommendation.get_cold_start_types")
@patch("recommendation.save_recommendation")
@patch("recommendation.create_db_connection")
def test_get_final_recommendation_for_group_b(
    connection_mock: Mock,
    save_recommendation_mock: Mock,
    get_cold_start_types: Mock,
    get_accessible_departements_from_coordinates: Mock,
    get_scored_recommendation_for_user_mock: Mock,
    get_intermediate_recommendations_for_user_mock: Mock,
    setup_database: Any,
    app_config: Dict[str, Any],
):
    # Given
    connection_mock.return_value = setup_database
    user_id = 112
    get_intermediate_recommendations_for_user_mock.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2"},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3"},
    ]
    get_scored_recommendation_for_user_mock.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3", "score": 3},
    ]
    get_accessible_departements_from_coordinates.return_value = ["93"]

    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )

    # Then
    get_cold_start_types.assert_not_called()
    save_recommendation_mock.assert_called()
    assert recommendations == [3, 2]


@patch("recommendation.order_offers_by_score_and_diversify_types")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_cold_start_ordered_recommendations")
@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_cold_start_types")
@patch("recommendation.get_accessible_departements_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("recommendation.create_db_connection")
def test_get_final_recommendation_for_new_user(
    connection_mock: Mock,
    save_recommendation_mock: Mock,
    get_accessible_departements_from_coordinates: Mock,
    get_cold_start_types: Mock,
    get_intermediate_recommendations_for_user: Mock,
    get_cold_start_ordered_recommendations: Mock,
    get_scored_recommendation_for_user: Mock,
    order_offers_by_score_and_diversify_types: Mock,
    setup_database: Any,
    app_config: Dict[str, Any],
):
    # Given
    connection_mock.return_value = setup_database
    user_id = 113
    get_cold_start_types.return_value = ["type2", "type3"]
    get_intermediate_recommendations_for_user.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "score": 2, "item_id": "offer-2"},
        {"id": 3, "url": "url3", "type": "type3", "score": 3, "item_id": "offer-3"},
    ]
    get_scored_recommendation_for_user.return_value = [
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3", "score": 3},
    ]
    get_cold_start_ordered_recommendations.return_value = [3, 2]
    order_offers_by_score_and_diversify_types.return_value = [3, 2]
    get_accessible_departements_from_coordinates.return_value = ["93"]

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )
    get_intermediate_recommendations_for_user.assert_called()
    save_recommendation_mock.assert_called()

    # Then
    assert recommendations == [3, 2]


@pytest.mark.parametrize(
    ["is_cold_start", "cold_start_types", "expected_recommendation"],
    [
        (
            False,
            [],
            [
                {"id": "2", "type": "B", "url": None, "item_id": "offer-2"},
                {"id": "3", "type": "C", "url": "url", "item_id": "offer-3"},
                {"id": "5", "type": "E", "url": None, "item_id": "offer-5"},
                {"id": "6", "type": "B", "url": None, "item_id": "offer-6"},
            ],
        ),
        (
            True,
            [],
            [
                {"id": "3", "type": "C", "url": "url", "item_id": "offer-3"},
                {"id": "6", "type": "B", "url": None, "item_id": "offer-6"},
                {"id": "2", "type": "B", "url": None, "item_id": "offer-2"},
                {"id": "5", "type": "E", "url": None, "item_id": "offer-5"},
            ],
        ),
        (
            True,
            "B",
            [
                {"id": "6", "type": "B", "url": None, "item_id": "offer-6"},
                {"id": "2", "type": "B", "url": None, "item_id": "offer-2"},
                {"id": "3", "type": "C", "url": "url", "item_id": "offer-3"},
                {"id": "5", "type": "E", "url": None, "item_id": "offer-5"},
            ],
        ),
    ],
)
def test_get_intermediate_recommendation_for_user(
    setup_database: Any, is_cold_start, cold_start_types, expected_recommendation
):
    # Given
    connection = setup_database

    # When
    user_id = 111
    user_accessible_department = ["93"]
    user_recommendation = get_intermediate_recommendations_for_user(
        user_id, user_accessible_department, is_cold_start, cold_start_types, connection
    )

    # Then
    assert_array_equal(
        user_recommendation
        if is_cold_start
        else sorted(user_recommendation, key=lambda k: k["id"]),
        expected_recommendation,
    )


@pytest.mark.parametrize(
    ["is_cold_start", "cold_start_types", "expected_recommendation"],
    [
        (
            False,
            [],
            [
                {"id": "1", "type": "A", "url": None, "item_id": "offer-1"},
                {"id": "3", "type": "C", "url": "url", "item_id": "offer-3"},
                {"id": "4", "type": "D", "url": "url", "item_id": "offer-4"},
                {"id": "5", "type": "E", "url": None, "item_id": "offer-5"},
            ],
        ),
        (
            True,
            [],
            [
                {"id": "3", "type": "C", "url": "url", "item_id": "offer-3"},
                {"id": "1", "type": "A", "url": None, "item_id": "offer-1"},
                {"id": "4", "type": "D", "url": "url", "item_id": "offer-4"},
                {"id": "5", "type": "E", "url": None, "item_id": "offer-5"},
            ],
        ),
        (
            True,
            ["A", "C"],
            [
                {"id": "3", "type": "C", "url": "url", "item_id": "offer-3"},
                {"id": "1", "type": "A", "url": None, "item_id": "offer-1"},
                {"id": "4", "type": "D", "url": "url", "item_id": "offer-4"},
                {"id": "5", "type": "E", "url": None, "item_id": "offer-5"},
            ],
        ),
    ],
)
def test_get_intermediate_recommendation_for_user_with_no_department(
    setup_database: Any, is_cold_start, cold_start_types, expected_recommendation
):
    # Given
    connection = setup_database

    # When
    user_id = 222
    user_accessible_department = []
    user_recommendation = get_intermediate_recommendations_for_user(
        user_id, user_accessible_department, is_cold_start, cold_start_types, connection
    )

    # Then
    assert_array_equal(
        user_recommendation
        if is_cold_start
        else sorted(user_recommendation, key=lambda k: k["id"]),
        expected_recommendation,
    )


@pytest.mark.parametrize(
    ["offers", "output"],
    [
        (
            [
                {"id": 1, "url": None, "type": "A", "item_id": "offer-1", "score": 1},
                {"id": 2, "url": None, "type": "A", "item_id": "offer-2", "score": 1},
                {"id": 3, "url": None, "type": "B", "item_id": "offer-3", "score": 10},
                {"id": 4, "url": None, "type": "B", "item_id": "offer-4", "score": 10},
            ],
            [4, 2, 3, 1],
        ),
        (
            [
                {"id": 1, "url": None, "type": "A", "item_id": "offer-1", "score": 1},
                {"id": 2, "url": None, "type": "A", "item_id": "offer-2", "score": 2},
                {"id": 3, "url": None, "type": "A", "item_id": "offer-3", "score": 10},
                {"id": 4, "url": None, "type": "A", "item_id": "offer-4", "score": 11},
            ],
            [4, 3, 2, 1],
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
        {"id": 1, "url": "url1", "type": "type1", "item_id": "offer-1"},
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2"},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3"},
    ]

    # When
    scored_recommendation_for_user = get_scored_recommendation_for_user(
        user_recommendation,
        app_config["MODEL_REGION"],
        app_config["MODEL_NAME"],
        app_config["MODEL_VERSION"],
    )

    # Then
    assert scored_recommendation_for_user == [
        {"id": 1, "url": "url1", "type": "type1", "item_id": "offer-1", "score": 1},
        {"id": 2, "url": "url2", "type": "type2", "item_id": "offer-2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "item_id": "offer-3", "score": 3},
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
