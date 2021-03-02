def get_iris_from_coordinates(longitude: float, latitude: float, connection) -> int:

    if not (longitude and latitude):
        return None

    iris_query = f"""SELECT id FROM iris_france
        WHERE ST_CONTAINS(shape, ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326))
        ORDER BY id;"""

    result = connection.execute(iris_query).fetchone()
    if result:
        iris_id = result[0]
    else:
        iris_id = None

    return iris_id
