from pcreco.utils.db.db_connection import get_db
from sqlalchemy import text


def get_cold_start_status(User) -> bool:
    bookings_count, clicks_count, favorites_count = _get_user_app_interaction(User)
    if User.group_id == "C":
        user_app_interaction_count = (
            bookings_count * 10 + favorites_count * 3 + clicks_count
        )
        user_cold_start_status = (user_app_interaction_count < 20) and not (
            _is_trained_user(User)
        )
    if User.group_id == "B":
        user_cold_start_status = clicks_count < 20
    else:
        user_cold_start_status = bookings_count < 2
    return user_cold_start_status


def _get_user_app_interaction(User) -> int:
    connection = get_db()
    app_interaction_query = text(
        f"""
            SELECT booking_cnt,consult_offer,has_added_offer_to_favorites
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


def _is_trained_user(User) -> bool:
    connection = get_db()
    is_trained_user = connection.execute(
        text("SELECT user_id FROM trained_users_mf_reco WHERE user_id= :user_id"),
        user_id=str(User.id),
    ).scalar()
    return is_trained_user
