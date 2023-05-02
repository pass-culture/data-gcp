import json
import pytest
from unittest.mock import Mock, patch, MagicMock, call

from typing import Any, List

from pcreco.core.user import User
from pcreco.core.recommendation import Recommendation
from pcreco.models.reco.playlist_params import PlaylistParamsIn
from pcreco.core.utils.cold_start import get_cold_start_status
from pcreco.utils.env_vars import QPI_FOLDER


@pytest.mark.parametrize(
    ["user_id", "cold_start_status"],
    [
        ("111", False),
        ("112", True),
        ("113", True),
    ],
)
def test_get_cold_start_status(
    setup_database: Any,
    user_id: str,
    cold_start_status: bool,
):
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        # Given
        connection_mock.return_value = setup_database
        user = User(user_id)
        assert get_cold_start_status(user) == cold_start_status


@pytest.mark.parametrize(
    ["user_id", "cold_start_categories"],
    [
        (
            "111",
            [
                "SUPPORT_PHYSIQUE_FILM",
                "JEU_EN_LIGNE",
            ],
        ),
        (
            "112",
            ["SUPPORT_PHYSIQUE_FILM"],
        ),
        (
            "113",
            ["LIVRE_PAPIER"],
        ),
    ],
)
@patch("pcreco.core.utils.cold_start.get_cold_start_status")
def test_get_cold_start_categories(
    cold_start_status_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_categories: List[str],
):
    # Given
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        # Given
        connection_mock.return_value = setup_database

        # Force Scoring to be cold_start
        cold_start_status_mock.return_value = True

        user = User(user_id)
        input_reco = PlaylistParamsIn()
        scoring = Recommendation(user, input_reco)
        assert sorted(scoring.scoring.cold_start_categories) == sorted(
            cold_start_categories
        )


@pytest.mark.parametrize(
    ["user_id", "cold_start_categories"],
    [
        (
            "5727",
            ["PODCAST", "FESTIVAL", "FESTIVAL_LIVRE"],
        ),
    ],
)
@patch("pcreco.core.utils.qpi_live_ingestion._get_qpi_file_from_gcs")
@patch("pcreco.core.utils.cold_start.get_cold_start_status")
def test_get_cold_start_categories_from_gcs(
    cold_start_status_mock: Mock,
    cold_start_categories_from_file_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_categories: List[str],
):
    # Given
    with patch("pcreco.utils.db.db_connection.__get_session") as connection_mock:
        # Given
        connection_mock.return_value = setup_database
        # Force Scoring to be cold_start
        cold_start_status_mock.return_value = True
        todays_date = "17890714"
        filepath = f"{QPI_FOLDER}/qpi_answers_{todays_date}/user_id_{user_id}.jsonl"
        with open(filepath, "r") as f:
            cold_start_categories_from_file = json.load(f)
        cold_start_categories_from_file_mock.return_value = (
            cold_start_categories_from_file
        )
        user = User(user_id)
        input_reco = PlaylistParamsIn()
        scoring = Recommendation(user, input_reco)
        assert sorted(scoring.scoring.cold_start_categories) == sorted(
            cold_start_categories
        )
