import polars as pl
from unittest.mock import patch, Mock
from typing import List
from utils import N_RECO_DISPLAY, get_offline_recos, call_builder


def base_uri(input):
    return f"https://apireco.testing.passculture.team/similar_offers/{input}?token=test_token"


mock_recos = [f"reco_{i}" for i in range(2 * N_RECO_DISPLAY)]
input_data = pl.DataFrame(
    {
        "user_id": ["u1", "u2", "u3"],
        "offer_id": ["o1", "o2", "o3"],
        "venue_longitude": ["lg1", "lg2", None],
        "venue_latitude": ["lt1", "lt2", None],
    }
)


class TestOfflineRecommendation:
    @patch("utils.similar_offers")
    def test_offline_recommendation(
        self,
        similar_offers_mock: Mock,
    ):
        similar_offers_mock.return_value = mock_recos
        offline_recommendations = get_offline_recos(input_data)
        assert len(offline_recommendations) > 0, "Offline recommendation is not empty"
        for row in offline_recommendations.rows(named=True):
            assert isinstance(
                row["recommendations"], List
            ), "Offline recommendation output is a List"
            assert (
                len(row["recommendations"]) == N_RECO_DISPLAY
            ), f"""Offline recommendation is the right lenght for user {row["user_id"]}"""

    def test_call_builder(
        self,
    ):
        for row in input_data.rows(named=True):
            call = call_builder(
                row["offer_id"], row["venue_longitude"], row["venue_latitude"]
            )
            if row["venue_longitude"] is None or row["venue_latitude"] is None:
                assert call == base_uri(
                    row["offer_id"]
                ), "Call with no geoloc properly built"
            else:
                assert (
                    call
                    == base_uri(row["offer_id"])
                    + f"""&longitude={row["venue_longitude"]}&latitude={row["venue_latitude"]}"""
                ), "Call with geoloc properly built"
