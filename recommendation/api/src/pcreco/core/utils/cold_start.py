from pcreco.utils.db.db_connection import get_session
from sqlalchemy import text
from pcreco.core.utils.qpi_live_ingestion import get_cold_start_categories_from_gcs
from typing import List, Dict, Any

def get_cold_start_status(User) -> bool:
    bookings_count, clicks_count, favorites_count = _get_user_app_interaction(User)
    user_cold_start_status = not (bookings_count >= 2 or clicks_count >= 25)
    return user_cold_start_status


def _get_user_app_interaction(User) -> int:
    connection = get_session()
    app_interaction_query = text(
        f"""
            SELECT 
                COALESCE(booking_cnt, 0) as booking_cnt,
                COALESCE(consult_offer, 0) as consult_offer,
                COALESCE(has_added_offer_to_favorites, 0) as has_added_offer_to_favorites
            FROM public.enriched_user_mv
            WHERE user_id= :user_id;
            """
    )
    query_result = connection.execute(
        app_interaction_query, user_id=str(User.id)
    ).fetchone()
    bookings_count = query_result[0] if query_result is not None else 0
    clicks_count = query_result[1] if query_result is not None else 0
    favorites_count = query_result[2] if query_result is not None else 0
    return bookings_count, clicks_count, favorites_count


def get_cold_start_categories(user_id) -> List[str]:
    cold_start_query = text(
        f"""SELECT subcategories FROM qpi_answers_mv WHERE user_id = :user_id;"""
    )

    connection = get_session()
    query_result = connection.execute(
        cold_start_query,
        user_id=str(user_id),
    ).fetchall()

    if len(query_result) > 0:
        cold_start_categories = [res[0] for res in query_result]
    else:
        cold_start_categories = get_cold_start_categories_from_gcs(user_id)
    return cold_start_categories
