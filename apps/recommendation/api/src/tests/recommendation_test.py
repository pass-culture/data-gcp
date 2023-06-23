from unittest.mock import Mock, patch
import pytest
from typing import Any
from pcreco.core.user import User
from pcreco.core.model_engine.recommendation import Recommendation
from pcreco.models.reco.playlist_params import PlaylistParamsIn


class RecommendationTest:
    @pytest.mark.parametrize(
        ["user_id", "geoloc", "use_case"],
        [
            (
                "115",
                {"longitude": 2.331289, "latitude": 48.830719},
                "15_geoloc",
            ),
            (
                "116",
                {"longitude": 2.331289, "latitude": 48.830719},
                "1617_geoloc",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                "18_geoloc",
            ),
        ],
    )
    @patch("pcreco.core.utils.cold_start.get_cold_start_categories")
    def test_recommendation_cold_start(
        self,
        cold_start_categories_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        use_case,
    ):
        with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            cold_start_categories_mock.return_value = ["SEANCE_CINE"]
            user = User(user_id, longitude, latitude)
            input_reco = PlaylistParamsIn({"modelEndpoint": "top_offers"})
            scoring = Recommendation(user, input_reco)
            user_recommendations = scoring.get_scoring()
            assert (
                len(user_recommendations) > 0
            ), f"{use_case}: user_recommendations list is non empty"

    @pytest.mark.parametrize(
        ["user_id", "geoloc", "categories", "is_event", "use_case"],
        [
            (
                "118",
                {"longitude": None, "latitude": None},
                ["CINEMA"],
                True,
                "18_nogeoloc_CINEMA",
            ),
            (
                "118",
                {"longitude": None, "latitude": None},
                None,
                True,
                "18_nogeoloc_ALL",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                ["CINEMA"],
                True,
                "18_geoloc_CINEMA",
            ),
            (
                "118",
                {"longitude": None, "latitude": None},
                ["SPECTACLE"],
                True,
                "18_nogeoloc_SPECTACLE",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                ["SPECTACLE"],
                True,
                "18_geoloc_SPECTACLE",
            ),
        ],
    )
    @patch("pcreco.core.utils.cold_start.get_cold_start_categories")
    def test_recommendation_playlist_top_offers(
        self,
        cold_start_categories_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        categories,
        is_event,
        use_case,
    ):
        with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            user = User(user_id, longitude, latitude)
            cold_start_categories_mock.return_value = ["SEANCE_CINE"]
            input_reco = PlaylistParamsIn(
                {
                    "modelEndpoint": "top_offers",
                    "categories": categories,
                    "isEvent": is_event,
                }
            )

            scoring = Recommendation(user, params_in=input_reco)

            recommended_offers = scoring.scorer.get_scoring()
            recommendation_sgn = [
                reco["search_group_name"] for reco in recommended_offers
            ]

            assert (
                len(recommended_offers) > 0
            ), f"{use_case}: playlist should not be empty"
            assert set(recommendation_sgn) == set(
                categories
            ), f"{use_case}: recommended categories are expected"

            assert (
                input_reco.has_conditions == True
            ), f"{input_reco.json} should contain params"

    @pytest.mark.parametrize(
        ["user_id", "geoloc", "subcategories", "use_case"],
        [
            (
                "118",
                {"longitude": None, "latitude": None},
                ["SPECTACLE_REPRESENTATION"],
                "18_geoloc_SPECTACLE_REPRESENTATION",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                ["SPECTACLE_REPRESENTATION"],
                "18_no_geoloc_SPECTACLE_REPRESENTATION",
            ),
        ],
    )
    @patch("pcreco.core.utils.cold_start.get_cold_start_categories")
    def test_recommendation_playlist_top_offers(
        self,
        cold_start_categories_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        subcategories,
        use_case,
    ):
        with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            user = User(user_id, longitude, latitude)

            cold_start_categories_mock.return_value = ["SEANCE_CINE"]
            input_reco = PlaylistParamsIn(
                {
                    "modelEndpoint": "top_offers",
                    "subcategories": subcategories,
                }
            )

            scoring = Recommendation(user, params_in=input_reco)

            recommended_offers = scoring.scorer.get_scoring()
            recommendation_sgn = [reco["subcategory_id"] for reco in recommended_offers]

            if latitude is not None and longitude is not None:

                assert (
                    len(recommended_offers) > 0
                ), f"{use_case}: playlist should not be empty"
                assert set(recommendation_sgn) == set(
                    subcategories
                ), f"{use_case}: recommended subcategories are expected"
            elif input_reco.is_event == True:
                assert (
                    len(recommended_offers) == 0
                ), f"{use_case}: playlist should be empty"
            else:
                assert (
                    len(recommended_offers) > 0
                ), f"{use_case}: playlist should not be empty"

            assert (
                input_reco.has_conditions == True
            ), f"{input_reco.json} should contain params"

    @pytest.mark.parametrize(
        ["user_id", "geoloc", "sub_cat", "params_in", "use_case"],
        [
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                ["EVENEMENT_CINE"],
                {
                    "offerTypeList": [
                        {"key": "MOVIE", "value": "BOOLYWOOD"},
                    ],
                    "modelEndpoint": "top_offers",
                },
                "18_geoloc_EVENEMENT_CINE_MOVIE",
            ),
            (
                "118",
                {"longitude": None, "latitude": None},
                ["LIVRE_PAPIER"],
                {
                    "offerTypeList": [
                        {"key": "BOOK", "value": "Histoire"},
                    ],
                    "modelEndpoint": "top_offers",
                },
                "18_geoloc_LIVRE_PAPIER_MOVIE",
            ),
        ],
    )
    def test_recommendation_offer_type_list(
        self,
        setup_database: Any,
        user_id,
        sub_cat,
        geoloc,
        params_in,
        use_case,
    ):
        with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            user = User(user_id, longitude, latitude)
            input_reco = PlaylistParamsIn(params_in)

            scoring = Recommendation(user, params_in=input_reco)

            recommended_offers = scoring.scorer.get_scoring()
            recommendation_sgn = [reco["subcategory_id"] for reco in recommended_offers]

            assert (
                len(recommended_offers) > 0
            ), f"{use_case}: playlist should not be empty"

            assert set(recommendation_sgn) == set(
                sub_cat
            ), f"{use_case}: recommended subcategories are expected"

            assert (
                input_reco.has_conditions == True
            ), f"{input_reco.json} should contain params"
