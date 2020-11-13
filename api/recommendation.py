import os
from typing import List

import psycopg2

SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_PASSWORD = os.environ.get("SQL_BASE_PASSWORD")


def get_recommendations_for_user(
    user_id: int, number_of_recommendations: int
) -> List[int]:

    connection = psycopg2.connect(
        user=SQL_BASE_USER,
        password=SQL_BASE_PASSWORD,
        database=SQL_BASE,
        host=f"/cloudsql/{SQL_CONNECTION_NAME}",
    )

    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT id FROM recommendable_offers WHERE id NOT IN 
        (SELECT offer_id FROM non_recommendable_offers WHERE user_id = {user_id}) 
        ORDER BY id;
        """
    )

    user_recommendation = [
        row[0] for row in cursor.fetchmany(number_of_recommendations)
    ]

    cursor.close()
    connection.close()

    return user_recommendation
