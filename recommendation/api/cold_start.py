import time
from sqlalchemy import text

from utils import create_db_connection, log_duration

MACRO_CATEGORIES_TYPE_MAPPING = {
    "cinema": ["CINEMA"],
    "audiovisuel": ["FILM"],
    "jeux_videos": ["JEU"],
    "livre": ["LIVRE"],
    "musees_patrimoine": ["MUSEE"],
    "musique": ["MUSIQUE_LIVE", "MUSIQUE_ENREGISTREE"],
    "pratique_artistique": ["PRATIQUE_ART"],
    "spectacle_vivant": ["SPECTACLE"],
    "instrument": ["INSTRUMENT"],
    "presse": ["MEDIA"],
    "autre": ["CONFERENCE_RENCONTRE", "BEAUX_ARTS"],
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


def get_cold_start_categories(user_id: int) -> list:
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

    cold_start_categories = []
    if len(query_result) == 0:
        return []
    for category_index, category in enumerate(query_result[0]):
        if category:
            cold_start_categories.extend(
                MACRO_CATEGORIES_TYPE_MAPPING[qpi_answers_categories[category_index]]
            )
    log_duration("get_cold_start_categories", start)
    return cold_start_categories
