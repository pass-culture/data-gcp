import time

from sqlalchemy import text
import typing as t
from pcreco.utils.env_vars import log_duration, DEFAULT_RECO_RADIUS
from pcreco.utils.db.db_connection import get_session

RECO_RADIUS_DICT = {
    "0_25KM": 0,
    "25_50KM": 25_000,
    "50_100KM": 50_000,
    "100_150KM": 100_000,
    "150KM+": 150_000,
}


def distance_to_radius_bucket(distance_meters: int) -> t.List[str]:
    radius_buckets = []
    for bucket, min_dist in RECO_RADIUS_DICT.items():
        if distance_meters > min_dist:
            radius_buckets.append(bucket)
    if len(radius_buckets) == 0:
        return DEFAULT_RECO_RADIUS
    return radius_buckets


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
