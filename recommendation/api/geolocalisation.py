from typing import List

ACCESSIBLE_DEPARTMENTS = {
    "08": ["02", "08", "51", "55", "59"],
    "22": ["22", "29", "35", "56"],
    "25": ["21", "25", "39", "68", "70", "71", "90"],
    "29": ["22", "35", "29", "56"],
    "34": ["11", "12", "13", "30", "31", "34", "48", "66", "81", "84"],
    "35": ["22", "29", "35", "44", "49", "50", "53", "56"],
    "56": ["22", "29", "35", "44", "56"],
    "58": ["03", "18", "21", "45", "58", "71", "89"],
    "67": ["54", "55", "57", "67", "68", "88"],
    "71": ["01", "03", "21", "39", "42", "58", "69", "71"],
    "84": ["04", "07", "13", "26", "30", "83", "84"],
    "93": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "94": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "97": ["97", "971", "972", "973"],
    "973": ["97", "971", "972", "973"],
}


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


def get_departements_from_coordinates(
    longitude: float, latitude: float, connection
) -> str:

    if not (longitude and latitude):
        return None

    departement_query = f"""
        SELECT code_insee FROM departements 
        WHERE ST_CONTAINS(geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 0))
    """

    result = connection.execute(departement_query).fetchone()
    if result:
        departement = result[0]
    else:
        departement = None

    return departement


def get_accessible_departements_from_coordinates(
    longitude: float, latitude: float, connection
) -> List[str]:

    department = get_departements_from_coordinates(longitude, latitude, connection)
    if department is None:
        return []
    else:
        if department in ACCESSIBLE_DEPARTMENTS:
            return ACCESSIBLE_DEPARTMENTS[department]
        else:
            return [department]
