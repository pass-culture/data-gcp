from typing import Any, Dict, List, Tuple
from unittest.mock import Mock, patch

from numpy.testing import assert_array_equal
import pytest

from recommendation import (
    get_final_recommendations,
    get_cold_start_scored_recommendations_for_user,
    get_intermediate_recommendations_for_user,
    get_scored_recommendation_for_user,
    order_offers_by_score_and_diversify_categories,
    save_recommendation,
)
from utils import create_db_connection


@patch("recommendation.get_cold_start_scored_recommendations_for_user")
@patch("recommendation.get_iris_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("utils.create_pool")
def test_get_final_recommendation_for_group_a_cold_start(
    mock_pool: Mock,
    save_recommendation_mock: Mock,
    get_iris_from_coordinates_mock: Mock,
    get_cold_start_scored_recommendations_for_user_mock: Mock,
    setup_pool: Any,
    app_config: Dict[str, Any],
):
    # Given
    mock_pool.return_value = setup_pool

    user_id = 113
    get_cold_start_scored_recommendations_for_user_mock.return_value = [
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "product_id": "product-2",
            "score": "2",
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "product_id": "product-3",
            "score": "3",
        },
    ]
    get_iris_from_coordinates_mock.return_value = 1

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )

    # Then
    assert sorted(recommendations) == [2, 3]
    save_recommendation_mock.assert_called_once()


@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_iris_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("utils.create_pool")
def test_get_final_recommendation_for_group_a_algo(
    mock_pool: Mock,
    save_recommendation_mock: Mock,
    get_iris_from_coordinates_mock: Mock,
    get_scored_recommendation_for_user_mock: Mock,
    get_intermediate_recommendations_for_user_mock: Mock,
    setup_pool: Any,
    app_config: Dict[str, Any],
):
    # Given
    mock_pool.return_value = setup_pool

    user_id = 111
    get_intermediate_recommendations_for_user_mock.return_value = [
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "item_id": "offer-2",
            "product_id": "product-2",
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "item_id": "offer-3",
            "product_id": "product-3",
        },
    ]
    get_scored_recommendation_for_user_mock.return_value = [
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "item_id": "offer-2",
            "product_id": "product-2",
            "score": 2,
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "item_id": "offer-3",
            "product_id": "product-3",
            "score": 3,
        },
    ]
    get_iris_from_coordinates_mock.return_value = 1

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )

    # Then
    assert sorted(recommendations) == [2, 3]
    save_recommendation_mock.assert_called_once()


@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_iris_from_coordinates")
@patch("recommendation.get_cold_start_categories")
@patch("recommendation.save_recommendation")
@patch("utils.create_pool")
# def test_get_final_recommendation_for_group_b(
#    mock_pool: Mock,
#    save_recommendation_mock: Mock,
#    get_cold_start_categories: Mock,
#    get_iris_from_coordinates_mock: Mock,
#    get_scored_recommendation_for_user_mock: Mock,
#    get_intermediate_recommendations_for_user_mock: Mock,
#    setup_pool: Any,
#    app_config: Dict[str, Any],
# ):
# Given
#   mock_pool.return_value = setup_pool

#    user_id = 112
#    get_intermediate_recommendations_for_user_mock.return_value = [
#        {
#            "id": 2,
#            "url": "url2",
#            "category": "category2",
#            "item_id": "offer-2",
#            "product_id": "product-2",
#        },
#        {
#            "id": 3,
#            "url": "url3",
#            "category": "category3",
#            "item_id": "offer-3",
#            "product_id": "product-3",
#        },
#    ]
#    get_scored_recommendation_for_user_mock.return_value = [
#        {
#            "id": 2,
#            "url": "url2",
#            "category": "category2",
#            "item_id": "offer-2",
#            "product_id": "product-2",
#            "score": 2,
#        },
#        {
#            "id": 3,
#            "url": "url3",
#            "category": "category3",
#            "item_id": "offer-3",
#            "product_id": "product-3",
#            "score": 3,
#        },
#    ]
#    get_iris_from_coordinates_mock.return_value = 1

#    recommendations = get_final_recommendations(
#        user_id, 2.331289, 48.830719, app_config
#    )

# Then
#    get_cold_start_categories.assert_not_called()
#    save_recommendation_mock.assert_called()
#    assert recommendations == [3, 2]


@patch("recommendation.order_offers_by_score_and_diversify_categories")
@patch("recommendation.get_scored_recommendation_for_user")
@patch("recommendation.get_cold_start_scored_recommendations_for_user")
@patch("recommendation.get_intermediate_recommendations_for_user")
@patch("recommendation.get_cold_start_categories")
@patch("recommendation.get_iris_from_coordinates")
@patch("recommendation.save_recommendation")
@patch("utils.create_pool")
def test_get_final_recommendation_for_new_user(
    mock_pool: Mock,
    save_recommendation_mock: Mock,
    get_iris_from_coordinates_mock: Mock,
    get_cold_start_categories: Mock,
    get_intermediate_recommendations_for_user: Mock,
    get_cold_start_scored_recommendations_for_user: Mock,
    get_scored_recommendation_for_user: Mock,
    order_offers_by_score_and_diversify_categories: Mock,
    setup_pool: Any,
    app_config: Dict[str, Any],
):
    # Given
    mock_pool.return_value = setup_pool

    user_id = 113
    get_cold_start_categories.return_value = ["category2", "category3"]
    get_intermediate_recommendations_for_user.return_value = [
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "score": 2,
            "item_id": "offer-2",
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "score": 3,
            "item_id": "offer-3",
        },
    ]
    get_scored_recommendation_for_user.return_value = [
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "item_id": "offer-2",
            "product_id": "product-2",
            "score": 2,
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "item_id": "offer-3",
            "product_id": "product-3",
            "score": 3,
        },
    ]
    get_cold_start_scored_recommendations_for_user.return_value = [3, 2]
    order_offers_by_score_and_diversify_categories.return_value = [3, 2]
    get_iris_from_coordinates_mock.return_value = 1

    # When
    recommendations = get_final_recommendations(
        user_id, 2.331289, 48.830719, app_config
    )
    try:
        get_intermediate_recommendations_for_user.assert_called()
    except AssertionError:
        get_cold_start_scored_recommendations_for_user.assert_called()
    # User should be either in cold start or algorithme

    save_recommendation_mock.assert_called()

    # Then
    assert recommendations == [3, 2]


@patch("recommendation.create_db_connection")
def test_get_intermediate_recommendation_for_user(
    connection_mock: Mock, setup_database: Any
):
    # Given
    connection_mock.return_value = setup_database
    # When
    user_id = 111
    user_iris_id = 1
    user_recommendation = get_intermediate_recommendations_for_user(
        user_id, user_iris_id
    )

    # Then
    assert_array_equal(
        sorted(user_recommendation, key=lambda k: k["id"]),
        [
            {
                "id": "2",
                "category": "B",
                "url": None,
                "item_id": "offer-2",
                "product_id": "product-2",
            },
            {
                "id": "3",
                "category": "C",
                "url": "url",
                "item_id": "offer-3",
                "product_id": "product-3",
            },
            {
                "id": "4",
                "category": "D",
                "url": "url",
                "item_id": "offer-4",
                "product_id": "product-4",
            },
            {
                "id": "5",
                "category": "E",
                "url": None,
                "item_id": "offer-5",
                "product_id": "product-5",
            },
            {
                "id": "6",
                "category": "B",
                "url": None,
                "item_id": "offer-6",
                "product_id": "product-6",
            },
        ],
    )


@patch("random.random")
@patch("recommendation.create_db_connection")
def test_get_cold_start_scored_recommendations_for_user(
    connection_mock: Mock, random_mock: Mock, setup_database: Any
):
    # Given
    connection_mock.return_value = setup_database
    random_mock.return_value = 1

    # When
    user_id = 113
    user_iris_id = None
    cold_start_categories = []
    number_of_preselected_offers = 3
    user_recommendation = get_cold_start_scored_recommendations_for_user(
        user_id,
        user_iris_id,
        cold_start_categories,
        number_of_preselected_offers,
    )

    # Then
    assert_array_equal(
        user_recommendation,
        [
            {
                "id": "3",
                "category": "C",
                "url": "url",
                "product_id": "product-3",
                "score": 1,
            },
            {
                "id": "1",
                "category": "A",
                "url": None,
                "product_id": "product-1",
                "score": 1,
            },
            {
                "id": "4",
                "category": "D",
                "url": "url",
                "product_id": "product-4",
                "score": 1,
            },
        ],
    )


@patch("recommendation.create_db_connection")
def test_get_intermediate_recommendation_for_user_with_no_iris(
    connection_mock: Mock,
    setup_database: Any,
):
    # Given
    connection_mock.return_value = setup_database

    # When
    user_id = 222
    user_iris_id = None
    user_recommendation = get_intermediate_recommendations_for_user(
        user_id, user_iris_id
    )

    # Then
    assert_array_equal(
        sorted(user_recommendation, key=lambda k: k["id"]),
        [
            {
                "id": "1",
                "category": "A",
                "url": None,
                "item_id": "offer-1",
                "product_id": "product-1",
            },
            {
                "id": "3",
                "category": "C",
                "url": "url",
                "item_id": "offer-3",
                "product_id": "product-3",
            },
            {
                "id": "4",
                "category": "D",
                "url": "url",
                "item_id": "offer-4",
                "product_id": "product-4",
            },
            {
                "id": "5",
                "category": "E",
                "url": None,
                "item_id": "offer-5",
                "product_id": "product-5",
            },
        ],
    )


@pytest.mark.parametrize(
    ["offers", "output"],
    [
        (
            [
                {
                    "id": 1,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-1",
                    "product_id": "product-1",
                    "score": 1,
                },
                {
                    "id": 2,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-2",
                    "product_id": "product-2",
                    "score": 1,
                },
                {
                    "id": 3,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-3",
                    "product_id": "product-3",
                    "score": 10,
                },
                {
                    "id": 4,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-4",
                    "product_id": "product-4",
                    "score": 10,
                },
            ],
            [4, 2, 3, 1],
        ),
        (
            [
                {
                    "id": 1,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-1",
                    "product_id": "product-1",
                    "score": 1,
                },
                {
                    "id": 2,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-2",
                    "product_id": "product-2",
                    "score": 2,
                },
                {
                    "id": 3,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-3",
                    "product_id": "product-3",
                    "score": 10,
                },
                {
                    "id": 4,
                    "url": None,
                    "category": "A",
                    "item_id": "offer-4",
                    "product_id": "product-4",
                    "score": 11,
                },
            ],
            [4, 3, 2, 1],
        ),
    ],
)
def test_order_offers_by_score_and_diversify_categories(
    offers: List[Dict[str, Any]], output: List[int]
):
    assert_array_equal(
        output, order_offers_by_score_and_diversify_categories(offers, 10)
    )


@patch("recommendation.predict_score")
def test_get_scored_recommendation_for_user(
    predict_score_mock: Mock, app_config: Dict[str, Any]
):
    # Given
    group_id = "A"
    user_id = 333
    predict_score_mock.return_value = [1, 2, 3]
    user_recommendation = [
        {
            "id": 1,
            "url": "url1",
            "category": "category1",
            "item_id": "offer-1",
            "product_id": "product-1",
        },
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "item_id": "offer-2",
            "product_id": "product-2",
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "item_id": "offer-3",
            "product_id": "product-3",
        },
    ]

    # When
    scored_recommendation_for_user = get_scored_recommendation_for_user(
        user_id,
        user_recommendation,
        app_config["MODEL_REGION"],
        app_config[f"MODEL_NAME_{group_id}"],
        app_config[f"MODEL_VERSION_{group_id}"],
        app_config[f"MODEL_INPUT_{group_id}"],
    )

    # Then
    assert scored_recommendation_for_user == [
        {
            "id": 1,
            "url": "url1",
            "category": "category1",
            "item_id": "offer-1",
            "product_id": "product-1",
            "score": 1,
        },
        {
            "id": 2,
            "url": "url2",
            "category": "category2",
            "item_id": "offer-2",
            "product_id": "product-2",
            "score": 2,
        },
        {
            "id": 3,
            "url": "url3",
            "category": "category3",
            "item_id": "offer-3",
            "product_id": "product-3",
            "score": 3,
        },
    ]


@patch("utils.create_pool")
def test_save_recommendation(
    mock_pool: Mock,
    setup_pool: Any,
):
    # Given
    mock_pool.return_value = setup_pool

    user_id = 1
    recommendations = [2, 3, 4]
    group_id = "A"
    reco_origin = "algo"

    # When
    save_recommendation(user_id, recommendations, group_id, reco_origin)

    # Then
    connection = create_db_connection()
    for offer_id in recommendations:
        query_result = connection.execute(
            f"""
            SELECT * FROM public.past_recommended_offers
            WHERE userid = {user_id}
            AND offerid = {offer_id}
            AND group_id = '{group_id}'
            AND reco_origin = '{reco_origin}'
            """
        ).fetchall()
        assert len(query_result) == 1
