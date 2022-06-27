import os
from unittest.mock import Mock, patch
import pytest
import random
from typing import Any
from pcreco.core.user import User
from pcreco.core.scoring import Scoring
from pcreco.models.reco.recommendation import RecommendationIn
import pandas as pd
from pcreco.utils.env_vars import ACTIVE_MODEL, ENV_SHORT_NAME


class RecommendationTest:
    # test_recommendation_algo
    @pytest.mark.parametrize(
        ["user_id", "geoloc", "model_name", "use_case"],
        [
            (
                "118",
                {"longitude": None, "latitude": None},
                f"tf_reco_{ENV_SHORT_NAME}",
                "18_nogeoloc",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                f"tf_reco_{ENV_SHORT_NAME}",
                "18_geoloc",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                f"deep_reco_{ENV_SHORT_NAME}",
                "18_geoloc",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                f"MF_reco_{ENV_SHORT_NAME}",
                "18_geoloc",
            ),
        ],
    )
    @patch("pcreco.core.scoring.Scoring.Algo.get_scored_offers")
    def test_recommendation_algo(
        self,
        get_scored_offers_algo_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        model_name,
        use_case,
    ):
        with patch(
            "pcreco.utils.db.db_connection.__create_db_connection"
        ) as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            user = User(user_id, longitude, latitude)

            input_reco = (
                RecommendationIn({"model_name": model_name}) if model_name else None
            )

            scoring = Scoring(user, recommendation_in=input_reco)
            recommendable_offers = scoring.scoring.recommendable_offers
            mock_predictions = [random.random()] * len(recommendable_offers)
            get_scored_offers_algo_mock.return_value = [
                {**recommendation, "score": mock_predictions[i]}
                for i, recommendation in enumerate(recommendable_offers)
            ]
            user_recommendations = scoring.get_recommendation()
            assert (
                len(user_recommendations) > 0
            ), f"{use_case}: user_recommendations list is non empty"

    # test_recommendation_cold_start
    @pytest.mark.parametrize(
        ["user_id", "geoloc", "use_case"],
        [
            (
                "118",
                {"longitude": None, "latitude": None},
                "18_nogeoloc",
            ),
            (
                "118",
                {"longitude": 2.331289, "latitude": 48.830719},
                "18_geoloc",
            ),
        ],
    )
    @patch("pcreco.core.scoring.get_cold_start_status")
    def test_recommendation_cold_start(
        self,
        cold_start_status_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        use_case,
    ):
        with patch(
            "pcreco.utils.db.db_connection.__create_db_connection"
        ) as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            cold_start_status_mock.return_value = True
            user = User(user_id, longitude, latitude)
            scoring = Scoring(user)
            user_recommendations = scoring.get_recommendation()
            assert (
                len(user_recommendations) > 0
            ), f"{use_case}: user_recommendations list is non empty"

    # test_recommendation_playlist_algo
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
    @patch("pcreco.core.scoring.Scoring.Algo.get_scored_offers")
    def test_recommendation_playlist_algo(
        self,
        get_scored_offers_algo_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        categories,
        is_event,
        use_case,
    ):
        with patch(
            "pcreco.utils.db.db_connection.__create_db_connection"
        ) as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            user = User(user_id, longitude, latitude)

            input_reco = RecommendationIn(
                {
                    "categories": categories,
                    "isEvent": is_event,
                    "model_name": ACTIVE_MODEL,
                }
            )

            scoring = Scoring(user, recommendation_in=input_reco)

            recommendable_offers = scoring.scoring.recommendable_offers
            recommendation_sgn = [
                reco["search_group_name"] for reco in recommendable_offers
            ]
            mock_predictions = [random.random()] * len(recommendable_offers)
            get_scored_offers_algo_mock.return_value = [
                {**recommendation, "score": mock_predictions[i]}
                for i, recommendation in enumerate(recommendable_offers)
            ]
            assert len(recommendable_offers) > 0, f"{use_case}: playlist is not empty"

            assert set(recommendation_sgn) == set(
                categories
            ), f"{use_case}: recommended categories are expected"

    # test_recommendation_playlist_cold_start
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
    @patch("pcreco.core.scoring.get_cold_start_status")
    def test_recommendation_playlist_cold_start(
        self,
        cold_start_status_mock: Mock,
        setup_database: Any,
        user_id,
        geoloc,
        categories,
        is_event,
        use_case,
    ):
        with patch(
            "pcreco.utils.db.db_connection.__create_db_connection"
        ) as connection_mock:
            connection_mock.return_value = setup_database
            longitude = geoloc["longitude"]
            latitude = geoloc["latitude"]

            user = User(user_id, longitude, latitude)
            cold_start_status_mock.return_value = True
            input_reco = RecommendationIn(
                {
                    "categories": categories,
                    "isEvent": is_event,
                }
            )

            scoring = Scoring(user, recommendation_in=input_reco)

            recommended_offers = scoring.scoring.get_scored_offers()
            recommendation_sgn = [
                reco["search_group_name"] for reco in recommended_offers
            ]
            assert len(recommended_offers) > 0, f"{use_case}: playlist is not empty"

            assert set(recommendation_sgn) == set(
                categories
            ), f"{use_case}: recommended categories are expected"
