import pytest
from unittest.mock import Mock, patch

from typing import Any, List

from src.pcreco.core.user import User
from src.pcreco.core.scoring import Scoring
from src.pcreco.core.utils.cold_start_status import get_cold_start_status


@pytest.mark.parametrize(
    ["user_id", "group_id", "cold_start_status"],
    [
        ("111", "A", False),
        ("112", "B", True),
        ("113", "C", True),
    ],
)
@patch("src.pcreco.utils.db.db_connection.create_db_connection")
def test_get_cold_start_status(
    connection_mock: Mock,
    setup_database: Any,
    user_id: str,
    group_id: str,
    cold_start_status: bool,
):
    # Given
    connection_mock.return_value = setup_database
    assert get_cold_start_status(user_id, group_id) == cold_start_status


@pytest.mark.parametrize(
    ["user_id", "cold_start_categories"],
    [
        (
            "111",
            [
                "BEAUX_ARTS",
                "CINEMA",
                "CONFERENCE_RENCONTRE",
                "FILM",
                "INSTRUMENT",
                "JEU",
                "LIVRE",
                "MEDIA",
                "MUSEE",
                "MUSIQUE_ENREGISTREE",
                "MUSIQUE_LIVE",
                "PRATIQUE_ART",
                "SPECTACLE",
                "TECHNIQUE",
            ],
        ),
        (
            "112",
            [
                "CINEMA",
                "CONFERENCE_RENCONTRE",
                "FILM",
                "INSTRUMENT",
                "JEU",
                "LIVRE",
                "MEDIA",
                "MUSEE",
                "MUSIQUE_ENREGISTREE",
                "MUSIQUE_LIVE",
                "PRATIQUE_ART",
                "SPECTACLE",
                "TECHNIQUE",
            ],
        ),
        (
            "113",
            [
                "CINEMA",
                "CONFERENCE_RENCONTRE",
                "FILM",
                "INSTRUMENT",
                "JEU",
                "LIVRE",
                "MEDIA",
                "MUSEE",
                "MUSIQUE_ENREGISTREE",
                "MUSIQUE_LIVE",
                "PRATIQUE_ART",
                "SPECTACLE",
                "TECHNIQUE",
            ],
        ),
    ],
)
@patch("src.pcreco.utils.db.db_connection.create_db_connection")
def test_get_cold_start_categories(
    connection_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_categories: List[str],
):
    # Given
    user = User(user_id)
    scoring = Scoring(user)
    connection_mock.return_value = setup_database
    assert sorted(scoring.ColdStart.get_cold_start_categories()) == sorted(
        cold_start_categories
    )
