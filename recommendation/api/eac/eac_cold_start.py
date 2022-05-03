import random
import time

from sqlalchemy import text

from utils import (
    create_db_connection,
    Playlist_args,
    NUMBER_OF_PRESELECTED_OFFERS,
    ENV_SHORT_NAME,
    log_duration,
)
from typing import Any, Dict, List
from eac.eac_scoring import (
    get_intermediate_recommendations_for_user_eac,
    get_scored_recommendation_for_user_eac,
)


def get_user_age(
    user_id,
):
    start = time.time()
    with create_db_connection() as connection:
        request_response = connection.execute(
            text(
                f"SELECT FLOOR(DATE_PART('DAY',user_deposit_creation_date - user_birth_date)/365) "
                f"FROM public.enriched_user "
                f"WHERE user_id = '{str(user_id)}' "
            )
        ).scalar()
    log_duration(f"get_user_age for {user_id}", start)
    return request_response


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


def get_cold_start_status_eac(user_id: int, group_id: str) -> bool:
    app_interaction_type = ["bookings", "clicks", "favorites"]
    app_interaction_count = []
    with create_db_connection() as connection:
        for app_interaction in app_interaction_type:
            cold_start_query = text(
                f"""
            SELECT {app_interaction}_count
            FROM number_of_{app_interaction}_per_user
            WHERE user_id= :user_id;
            """
            )
            query_result = connection.execute(
                cold_start_query, user_id=str(user_id)
            ).fetchone()
            result = query_result[0] if query_result is not None else 0
            app_interaction_count.append(result)
        bookings_count = app_interaction_count[0]
        clicks_count = app_interaction_count[1]
        favorites_count = app_interaction_count[2]
        if group_id == "C":
            user_app_interaction_count = (
                bookings_count * 10 + favorites_count * 3 + clicks_count
            )
            is_trained_user = connection.execute(
                text(
                    f"SELECT user_id FROM trained_users_mf_reco WHERE user_id= :user_id"
                ),
                user_id=str(user_id),
            ).scalar()
            user_cold_start_status = (user_app_interaction_count < 20) and not (
                is_trained_user
            )
        if group_id == "B":
            user_cold_start_status = clicks_count < 20
        else:
            user_cold_start_status = bookings_count < 2
    return user_cold_start_status


def get_cold_start_categories_eac(user_id: int) -> list:

    qpi_answers_categories = list(MACRO_CATEGORIES_TYPE_MAPPING.keys())
    cold_start_query = text(
        f"""SELECT {'"' + '","'.join(qpi_answers_categories) + '"'} FROM qpi_answers WHERE user_id = :user_id;"""
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
    return list(set(cold_start_categories))


def get_cold_start_scored_recommendations_for_user_eac(
    user_id: int,
    user_iris_id: int,
    cold_start_categories: list,
    playlist_args_json=None,
) -> List[Dict[str, Any]]:
    recommendations_for_user = get_intermediate_recommendations_for_user_eac(
        user_id, user_iris_id, playlist_args_json
    )
    # here we change user_id for cs user_id and put group C to get mf_reco model
    user_age = int(get_user_age(user_id))
    print("user_age", user_age)
    user_id_CS = f"eac{user_age}"
    cold_start_recommendations = get_scored_recommendation_for_user_eac(
        user_id_CS, "C", recommendations_for_user
    )
    return cold_start_recommendations
