from sqlalchemy.orm import Session
from sqlalchemy import func
from geoalchemy2.elements import WKTElement

from schemas.offer import Offer
from schemas.user import User
from schemas.item import Item

from models.item_ids_mv import ItemIdsMv
from models.recommendable_offers_raw_mv import RecommendableOffersRaw

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


def get_nearest_offer(db: Session, user: User, item: Item) -> Offer:
    """Query the database to get the nearest offer from user given a recommendable item."""

    if user.latitude is not None and user.longitude is not None:
        user_geolocated = True
        user_point = WKTElement(f"POINT({user.latitude} {user.longitude})")
    else:
        user_geolocated = False

    if user_geolocated:
        offers = (
            db.query(RecommendableOffersRaw.offer_id, RecommendableOffersRaw.item_id)
            .filter(RecommendableOffersRaw.item_id == item.item_id)
            .order_by(
                func.ST_Distance(
                    user_point,
                    func.Geometry(
                        func.ST_MakePoint(
                            RecommendableOffersRaw.venue_longitude,
                            RecommendableOffersRaw.venue_latitude,
                        )
                    ),
                )
            )
            .limit(1)
            .all()
        )
    else:
        offers = (
            db.query(RecommendableOffersRaw.offer_id, RecommendableOffersRaw.item_id)
            .filter(RecommendableOffersRaw.item_id == item.item_id)
            .limit(1)
            .all()
        )

    offer = [
        Offer(
            offer_id=offer_id,
            item_id=item_id,
        )
        for offer_id, item_id in offers
    ]

    return offer
