from sqlalchemy import func
from sqlalchemy.orm import Session
from geoalchemy2.elements import WKTElement

from huggy.models.iris_france import IrisFrance


def get_iris_from_coordinates(db: Session, latitude, longitude) -> str:
    """Query the database in ORM mode to get iris_id from a set of coordinates."""
    point = WKTElement(f"POINT({latitude} {longitude})")
    iris_id = (
        db.query(IrisFrance.id)
        .filter(func.ST_Contains(IrisFrance.shape, point))
        .first()
    )
    if iris_id:
        return iris_id[0]
