import time
from sqlalchemy import text

from utils import create_db_connection, log_duration

MACRO_CATEGORIES_TYPE_MAPPING = {
    "cinema": ["EventType.CINEMA", "ThingType.CINEMA_CARD", "ThingType.CINEMA_ABO"],
    "audiovisuel": ["ThingType.AUDIOVISUEL"],
    "jeux_videos": ["ThingType.JEUX_VIDEO_ABO", "ThingType.JEUX_VIDEO"],
    "livre": ["ThingType.LIVRE_EDITION", "ThingType.LIVRE_AUDIO"],
    "musees_patrimoine": [
        "EventType.MUSEES_PATRIMOINE",
        "ThingType.MUSEES_PATRIMOINE_ABO",
    ],
    "musique": ["EventType.MUSIQUE", "ThingType.MUSIQUE_ABO", "ThingType.MUSIQUE"],
    "pratique_artistique": [
        "EventType.PRATIQUE_ARTISTIQUE",
        "ThingType.PRATIQUE_ARTISTIQUE_ABO",
    ],
    "spectacle_vivant": [
        "EventType.SPECTACLE_VIVANT",
        "ThingType.SPECTACLE_VIVANT_ABO",
    ],
    "instrument": ["ThingType.INSTRUMENT"],
    "presse": ["ThingType.PRESSE_ABO"],
    "autre": ["EventType.CONFERENCE_DEBAT_DEDICACE"],
}


def get_cold_start_status(user_id: int) -> bool:
    start = time.time()
    cold_start_query = text(
        """
        SELECT bookings_count
        FROM number_of_bookings_per_user
        WHERE user_id= :user_id;
        """
    )

    with create_db_connection() as connection:
        query_result = connection.execute(
            cold_start_query, user_id=str(user_id)
        ).fetchone()

    bookings_count = query_result[0] if query_result is not None else 0
    user_cold_start_status = bookings_count < 2
    log_duration(f"get_cold_start_status", start)
    return user_cold_start_status


def get_cold_start_types(user_id: int) -> list:
    start = time.time()
    qpi_answers_categories = [
        "cinema",
        "audiovisuel",
        "jeux_videos",
        "livre",
        "musees_patrimoine",
        "musique",
        "pratique_artistique",
        "spectacle_vivant",
        "instrument",
        "presse",
        "autre",
    ]
    cold_start_query = text(
        f"SELECT {', '.join(qpi_answers_categories)} FROM qpi_answers WHERE user_id = :user_id;"
    )

    with create_db_connection() as connection:
        query_result = connection.execute(
            cold_start_query,
            user_id=str(user_id),
        ).fetchall()

    cold_start_types = []
    if len(query_result) == 0:
        return []
    for category_index, category in enumerate(query_result[0]):
        if category:
            cold_start_types.extend(
                MACRO_CATEGORIES_TYPE_MAPPING[qpi_answers_categories[category_index]]
            )
    log_duration("get_cold_start_types", start)
    return cold_start_types
