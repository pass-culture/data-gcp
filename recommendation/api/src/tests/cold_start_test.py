import pytest
from unittest.mock import Mock, patch, MagicMock, call

from typing import Any, List

from pcreco.core.user import User
from pcreco.core.scoring import Scoring
from pcreco.core.utils.cold_start_status import get_cold_start_status


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
    with patch(
        "pcreco.utils.db.db_connection.__create_db_connection"
    ) as connection_mock:
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
@patch("pcreco.core.scoring.get_cold_start_status")
def test_get_cold_start_categories(
    cold_start_status_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_categories: List[str],
):
    # Given
    with patch(
        "pcreco.utils.db.db_connection.__create_db_connection"
    ) as connection_mock:
        # Given
        connection_mock.return_value = setup_database
        
        #Force Scoring to be cold_start
        cold_start_status_mock.return_value = True

        user = User(user_id)

        scoring = Scoring(user)
        assert sorted(
            scoring.scoring.get_cold_start_categories()
        ) == sorted(cold_start_categories)
