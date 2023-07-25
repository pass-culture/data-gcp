from sqlalchemy.orm import Session
from sqlalchemy import func

from models.item_ids_mv import ItemIdsMv
from schemas.offer import Offer
from crud.iris import get_iris_from_coordinates


def get_offer_characteristics(
    db: Session, offer_id: str, latitude: float, longitude: float
) -> Offer:
    """Query the database in ORM mode to get characteristics of an offer.
    Return : List[item_id,  number of booking associated].
    """
    offer_characteristics = (
        db.query(ItemIdsMv.item_id, ItemIdsMv.booking_number)
        .filter(ItemIdsMv.offer_id == offer_id)
        .first()
    )

    if latitude and longitude:
        iris_id = get_iris_from_coordinates(db, latitude, longitude)
    else:
        iris_id = None

    if offer_characteristics:
        offer = Offer(
            offer_id=offer_id,
            latitude=latitude,
            longitude=longitude,
            iris_id=iris_id,
            item_id=offer_characteristics[0],
            cnt_bookings=offer_characteristics[1],
        )
    else:
        offer = Offer(
            offer_id=offer_id,
            latitude=latitude,
            longitude=longitude,
            iris_id=iris_id,
        )
    return offer
