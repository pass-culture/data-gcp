import os
from unittest.mock import Mock, patch
import pytest
import random
from typing import Any
from pcreco.core.user import User
from pcreco.core.scoring import Scoring
from pcreco.models.reco.recommendation import RecommendationIn

ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME")


class RecommendationTest:
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
