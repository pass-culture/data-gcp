import time
from sqlalchemy import text

from utils import create_db_connection, log_duration

# build with notebook
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
        "ABO_BIBLIOTHEQUE",
        "ABO_CONCERT",
        "ABO_JEU_VIDEO",
        "ABO_LIVRE_NUMERIQUE",
        "ABO_LUDOTHEQUE",
        "ABO_MEDIATHEQUE",
        "ABO_MUSEE",
        "ABO_PLATEFORME_MUSIQUE",
        "ABO_PLATEFORME_VIDEO",
        "ABO_PRATIQUE_ART",
        "ABO_PRESSE_EN_LIGNE",
        "ABO_SPECTACLE",
        "ACHAT_INSTRUMENT",
        "ACTIVATION_EVENT",
        "ACTIVATION_THING",
        "APP_CULTURELLE",
        "ATELIER_PRATIQUE_ART",
        "AUTRE_SUPPORT_NUMERIQUE",
        "BON_ACHAT_INSTRUMENT",
        "CAPTATION_MUSIQUE",
        "CARTE_CINE_ILLIMITE",
        "CARTE_CINE_MULTISEANCES",
        "CARTE_MUSEE",
        "CINE_PLEIN_AIR",
        "CINE_VENTE_DISTANCE",
        "CONCERT",
        "CONCOURS",
        "CONFERENCE",
        "DECOUVERTE_METIERS",
        "ESCAPE_GAME",
        "EVENEMENT_CINE",
        "EVENEMENT_JEU",
        "EVENEMENT_MUSIQUE",
        "EVENEMENT_PATRIMOINE",
        "FESTIVAL_CINE",
        "FESTIVAL_LIVRE",
        "FESTIVAL_MUSIQUE",
        "FESTIVAL_SPECTACLE",
        "JEU_EN_LIGNE",
        "JEU_SUPPORT_PHYSIQUE",
        "LIVESTREAM_EVENEMENT",
        "LIVESTREAM_MUSIQUE",
        "LIVRE_AUDIO_PHYSIQUE",
        "LIVRE_NUMERIQUE",
        "LIVRE_PAPIER",
        "LOCATION_INSTRUMENT",
        "MATERIEL_ART_CREATIF",
        "MUSEE_VENTE_DISTANCE",
        "OEUVRE_ART",
        "PARTITION",
        "PODCAST",
        "RENCONTRE_JEU",
        "RENCONTRE",
        "SALON",
        "SEANCE_CINE",
        "SEANCE_ESSAI_PRATIQUE_ART",
        "SPECTACLE_ENREGISTRE",
        "SPECTACLE_REPRESENTATION",
        "SUPPORT_PHYSIQUE_FILM",
        "SUPPORT_PHYSIQUE_MUSIQUE",
        "TELECHARGEMENT_LIVRE_AUDIO",
        "TELECHARGEMENT_MUSIQUE",
        "VISITE_GUIDEE",
        "VISITE_VIRTUELLE",
        "VISITE",
        "VOD",
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
