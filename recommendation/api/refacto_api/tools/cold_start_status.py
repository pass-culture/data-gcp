from refacto_api.tools.db_connection import create_db_connection
from sqlalchemy import text


def get_cold_start_status(User):
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


def _get_user_app_interaction(User):
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
                cold_start_query, user_id=str(User.id)
            ).fetchone()
            result = query_result[0] if query_result is not None else 0
            app_interaction_count.append(result)
        bookings_count = app_interaction_count[0]
        clicks_count = app_interaction_count[1]
        favorites_count = app_interaction_count[2]
    return bookings_count, clicks_count, favorites_count


def _is_trained_user(User):
    with create_db_connection() as connection:
        is_trained_user = connection.execute(
            text("SELECT user_id FROM trained_users_mf_reco WHERE user_id= :user_id"),
            user_id=str(User.id),
        ).scalar()
    return is_trained_user
