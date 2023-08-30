from pcreco.utils.db.db_connection import get_session
from sqlalchemy import text
from pcreco.core.utils.qpi_live_ingestion import get_cold_start_categories_from_gcs
from typing import List


def get_cold_start_categories(user_id) -> List[str]:
    cold_start_query = text(
        f"""SELECT DISTINCT subcategories
            FROM qpi_answers_mv WHERE user_id = :user_id
            order by subcategories 
            ;"""
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
