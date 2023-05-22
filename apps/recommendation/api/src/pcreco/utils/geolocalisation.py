import time

from sqlalchemy import text
import typing as t
from pcreco.utils.env_vars import log_duration
from pcreco.utils.db.db_connection import get_session


def get_iris_from_coordinates(longitude: float, latitude: float) -> int:
    start = time.time()
    if not (longitude and latitude):
        return None

    iris_query = text(
        """
        SELECT id FROM iris_france
        WHERE ST_CONTAINS(ST_SetSRID(shape, 4326), ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326))
        ORDER BY id;
        """
    )

    connection = get_session()
    result = connection.execute(
        iris_query, longitude=longitude, latitude=latitude
    ).fetchone()

    if result:
        iris_id = result[0]
    else:
        iris_id = None
    log_duration(f"get_iris_from_coordinates", start)
    return iris_id
