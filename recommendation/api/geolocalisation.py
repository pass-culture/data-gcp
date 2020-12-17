import psycopg2

from api import SQL_BASE, SQL_BASE_PASSWORD, SQL_BASE_USER, SQL_CONNECTION_NAME


def get_iris_from_coordinates(
    longitude: float, latitude: float, connection=None
) -> int:
    close_connection = False
    if connection is None:
        connection = psycopg2.connect(
            user=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            host=f"/cloudsql/{SQL_CONNECTION_NAME}",
        )
        close_connection = True

    if not (longitude and latitude):
        if close_connection:
            connection.close()
        return None

    iris_query = f"""SELECT id FROM iris_france
        WHERE ST_CONTAINS(shape, ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326))
        ORDER BY id;"""

    cursor = connection.cursor()
    cursor.execute(iris_query)

    result = cursor.fetchone()
    if result:
        iris_id = result[0]
    else:
        iris_id = None

    if close_connection:
        cursor.close()
        connection.close()

    return iris_id
