import pytest
from unittest.mock import Mock, patch

from typing import Any, List

from not_eac.cold_start import get_cold_start_status, get_cold_start_categories


@pytest.mark.parametrize(
    ["user_id", "group_id", "cold_start_status"],
    [
        ("111", "A", False),
        ("112", "B", True),
        ("113", "C", False),
    ],
)
@patch("not_eac.cold_start.create_db_connection")
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
@patch("not_eac.cold_start.create_db_connection")
def test_get_cold_start_categories(
    connection_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_categories: List[str],
):
    # Given
    connection_mock.return_value = setup_database
    assert sorted(get_cold_start_categories(user_id)) == sorted(cold_start_categories)
