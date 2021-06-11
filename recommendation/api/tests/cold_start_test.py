import pytest
from unittest.mock import Mock, patch

from typing import Any, List

from cold_start import get_cold_start_status, get_cold_start_types


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
    ["user_id", "cold_start_types"],
    [
        (
            "111",
            [
                "EventType.CINEMA",
                "ThingType.CINEMA_CARD",
                "ThingType.CINEMA_ABO",
                "ThingType.AUDIOVISUEL",
                "ThingType.JEUX_VIDEO_ABO",
                "ThingType.JEUX_VIDEO",
                "ThingType.LIVRE_EDITION",
                "ThingType.LIVRE_AUDIO",
                "EventType.MUSEES_PATRIMOINE",
                "ThingType.MUSEES_PATRIMOINE_ABO",
                "EventType.MUSIQUE",
                "ThingType.MUSIQUE_ABO",
                "ThingType.MUSIQUE",
                "EventType.PRATIQUE_ARTISTIQUE",
                "ThingType.PRATIQUE_ARTISTIQUE_ABO",
                "EventType.SPECTACLE_VIVANT",
                "ThingType.SPECTACLE_VIVANT_ABO",
                "ThingType.INSTRUMENT",
                "ThingType.PRESSE_ABO",
                "EventType.CONFERENCE_DEBAT_DEDICACE",
            ],
        ),
        (
            "112",
            [
                "EventType.CINEMA",
                "ThingType.CINEMA_CARD",
                "ThingType.CINEMA_ABO",
                "ThingType.JEUX_VIDEO_ABO",
                "ThingType.JEUX_VIDEO",
                "EventType.MUSEES_PATRIMOINE",
                "ThingType.MUSEES_PATRIMOINE_ABO",
                "EventType.MUSIQUE",
                "ThingType.MUSIQUE_ABO",
                "ThingType.MUSIQUE",
                "EventType.PRATIQUE_ARTISTIQUE",
                "ThingType.PRATIQUE_ARTISTIQUE_ABO",
                "ThingType.PRESSE_ABO",
            ],
        ),
        ("113", []),
    ],
)
@patch("cold_start.create_db_connection")
def test_get_cold_start_types(
    connection_mock: Mock,
    setup_database: Any,
    user_id: str,
    cold_start_types: List[str],
):
    # Given
    connection_mock.return_value = setup_database
    assert sorted(get_cold_start_types(user_id)) == sorted(cold_start_types)
