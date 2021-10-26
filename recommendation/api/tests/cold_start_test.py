import pytest
from unittest.mock import Mock, patch

from typing import Any, List

from cold_start import get_cold_start_status, get_cold_start_categories


@pytest.mark.parametrize(
    ["user_id", "cold_start_status"],
    [
        ("111", False),
        ("112", True),
        ("113", True),
    ],
)
@patch("cold_start.create_db_connection")
def test_get_cold_start_status(
    connection_mock: Mock, setup_database: Any, user_id: str, cold_start_status: bool
):
    # Given
    connection_mock.return_value = setup_database
    assert get_cold_start_status(user_id) == cold_start_status


@pytest.mark.parametrize(
    ["user_id", "cold_start_categories"],
    [
        (
            "111",
            [
                "CINEMA",
                "FILM",
                "JEU",
                "LIVRE",
                "MUSEE",
                "MUSIQUE",
                "PRATIQUE_ART",
                "SPECTACLE",
                "INSTRUMENT",
                "MEDIA",
                "CONFERENCE_RENCONTRE",
                "BEAUX_ARTS",
            ],
        ),
        (
            "112",
            [
                "CINEMA",
                "JEU",
                "MUSEE",
                "MUSIQUE",
                "PRATIQUE_ART",
                "MEDIA",
            ],
        ),
        ("113", []),
    ],
)
@patch("cold_start.create_db_connection")
def test_get_cold_start_categories(
    connection_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_categories: List[str],
):
    # Given
    connection_mock.return_value = setup_database
    assert sorted(get_cold_start_categories(user_id)) == sorted(cold_start_categories)
