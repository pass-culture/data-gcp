import os
import psycopg2


SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")
SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_PASSWORD = os.environ.get("SQL_BASE_PASSWORD")


def get_iris_from_coordinates(
    longitude: float, latitude: float, connection=None
) -> int:
    if connection is None:
        connection = psycopg2.connect(
            user=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            host=f"/cloudsql/{SQL_CONNECTION_NAME}",
        )

    iris_query = f"""SELECT id FROM iris_france
        WHERE ST_CONTAINS(shape, ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326))
        ORDER BY id;"""

    cursor = connection.cursor()
    cursor.execute(iris_query)

    iris_id = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return iris_id
