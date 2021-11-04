import time
from sqlalchemy import text

from utils import create_db_connection, log_duration

# build with notebook , to improve use subcategories table in db
MACRO_CATEGORIES_TYPE_MAPPING = {
    "SUPPORT_PHYSIQUE_FILM": ["FILM"],
    "ABO_MEDIATHEQUE": ["FILM"],
    "VOD": ["FILM"],
    "ABO_PLATEFORME_VIDEO": ["FILM"],
    "AUTRE_SUPPORT_NUMERIQUE": ["FILM"],
    "CARTE_CINE_MULTISEANCES": ["CINEMA"],
    "CARTE_CINE_ILLIMITE": ["CINEMA"],
    "SEANCE_CINE": ["CINEMA"],
    "EVENEMENT_CINE": ["CINEMA"],
    "FESTIVAL_CINE": ["CINEMA"],
    "CINE_VENTE_DISTANCE": ["CINEMA"],
    "CINE_PLEIN_AIR": ["CINEMA"],
    "CONFERENCE": ["CONFERENCE_RENCONTRE"],
    "RENCONTRE": ["CONFERENCE_RENCONTRE"],
    "DECOUVERTE_METIERS": ["CONFERENCE_RENCONTRE"],
    "SALON": ["CONFERENCE_RENCONTRE"],
    "CONCOURS": ["JEU"],
    "RENCONTRE_JEU": ["JEU"],
    "ESCAPE_GAME": ["JEU"],
    "EVENEMENT_JEU": ["JEU"],
    "JEU_EN_LIGNE": ["JEU"],
    "ABO_JEU_VIDEO": ["JEU"],
    "ABO_LUDOTHEQUE": ["JEU"],
    "LIVRE_PAPIER": ["LIVRE"],
    "LIVRE_NUMERIQUE": ["LIVRE"],
    "TELECHARGEMENT_LIVRE_AUDIO": ["LIVRE"],
    "LIVRE_AUDIO_PHYSIQUE": ["LIVRE"],
    "ABO_BIBLIOTHEQUE": ["LIVRE"],
    "ABO_LIVRE_NUMERIQUE": ["LIVRE"],
    "FESTIVAL_LIVRE": ["LIVRE"],
    "CARTE_MUSEE": ["MUSEE"],
    "ABO_MUSEE": ["MUSEE"],
    "VISITE": ["MUSEE"],
    "VISITE_GUIDEE": ["MUSEE"],
    "EVENEMENT_PATRIMOINE": ["MUSEE"],
    "VISITE_VIRTUELLE": ["MUSEE"],
    "MUSEE_VENTE_DISTANCE": ["MUSEE"],
    "CONCERT": ["MUSIQUE_LIVE"],
    "EVENEMENT_MUSIQUE": ["MUSIQUE_LIVE"],
    "LIVESTREAM_MUSIQUE": ["MUSIQUE_LIVE"],
    "ABO_CONCERT": ["MUSIQUE_LIVE"],
    "FESTIVAL_MUSIQUE": ["MUSIQUE_LIVE"],
    "SUPPORT_PHYSIQUE_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "TELECHARGEMENT_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "ABO_PLATEFORME_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "CAPTATION_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "SEANCE_ESSAI_PRATIQUE_ART": ["PRATIQUE_ART"],
    "ATELIER_PRATIQUE_ART": ["PRATIQUE_ART"],
    "ABO_PRATIQUE_ART": ["PRATIQUE_ART"],
    "ABO_PRESSE_EN_LIGNE": ["MEDIA"],
    "PODCAST": ["MEDIA"],
    "APP_CULTURELLE": ["MEDIA"],
    "SPECTACLE_REPRESENTATION": ["SPECTACLE"],
    "SPECTACLE_ENREGISTRE": ["SPECTACLE"],
    "LIVESTREAM_EVENEMENT": ["SPECTACLE"],
    "FESTIVAL_SPECTACLE": ["SPECTACLE"],
    "ABO_SPECTACLE": ["SPECTACLE"],
    "ACHAT_INSTRUMENT": ["INSTRUMENT"],
    "BON_ACHAT_INSTRUMENT": ["INSTRUMENT"],
    "LOCATION_INSTRUMENT": ["INSTRUMENT"],
    "PARTITION": ["INSTRUMENT"],
    "MATERIEL_ART_CREATIF": ["BEAUX_ARTS"],
    "ACTIVATION_EVENT": ["TECHNIQUE"],
    "ACTIVATION_THING": ["TECHNIQUE"],
    "JEU_SUPPORT_PHYSIQUE": ["TECHNIQUE"],
    "OEUVRE_ART": ["TECHNIQUE"],
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

    qpi_answers_categories = list(MACRO_CATEGORIES_TYPE_MAPPING.keys())
    cold_start_query = text(
        f"""SELECT {', '.join(f'"{qpi_answers_categories}"')} FROM qpi_answers WHERE user_id = :user_id;"""
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
    return list(set(cold_start_categories))
