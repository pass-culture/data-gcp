def get_cold_start_status(user_id: int, connection) -> bool:
    cold_start_query = f"""
        SELECT count(*)
        FROM booking
        WHERE user_id = {user_id};
    """
    query_result = connection.execute(cold_start_query).fetchall()

    user_cold_start_status = query_result[0][0] < 3

    return user_cold_start_status


def get_cold_start_categories(user_id: int, connection) -> list:
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
    cold_start_query = f"""
        SELECT {', '.join(qpi_answers_categories)}
        FROM qpi_answers
        WHERE user_id = '{user_id}';
    """
    query_result = connection.execute(cold_start_query).fetchall()

    cold_start_categories = []
    if len(query_result) == 0:
        return []
    for category_index, category in enumerate(query_result[0]):
        if category:
            cold_start_categories.append(qpi_answers_categories[category_index])

    return cold_start_categories
