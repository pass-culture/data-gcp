from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from geoalchemy2.elements import WKTElement
from typing import List

from huggy.schemas.offer import Offer, RecommendableOffer
from huggy.schemas.user import User
from huggy.schemas.item import Item, RecommendableItem

from huggy.models.item_ids_mv import ItemIdsMv
from huggy.models.recommendable_offers_raw import get_available_table
from huggy.models.non_recommendable_items import NonRecommendableItems

from huggy.crud.iris import get_iris_from_coordinates
from huggy.utils.database import bind_engine


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
            is_geolocated=True if iris_id else False,
            item_id=offer_characteristics[0],
            booking_number=offer_characteristics[1],
            found=True,
        )
    else:
        offer = Offer(
            offer_id=offer_id,
            latitude=latitude,
            longitude=longitude,
            iris_id=iris_id,
            is_geolocated=True if iris_id else False,
            found=False,
        )
    return offer


def get_non_recommendable_items(db: Session, user: User) -> List[str]:

    non_recommendable_items = db.query(
        NonRecommendableItems.item_id.label("item_id")
    ).filter(NonRecommendableItems.user_id == user.user_id)

    return [
        recommendable_item.item_id for recommendable_item in non_recommendable_items
    ]


def get_nearest_offer(db: Session, user: User, item: RecommendableItem) -> Offer:
    """Query the database to get the nearest offer from user given a recommendable item."""

    offer_table = get_available_table(bind_engine, "RecommendableOffersRaw")

    if user.latitude is not None and user.longitude is not None:
        user_geolocated = True
        user_point = WKTElement(f"POINT({user.latitude} {user.longitude})")
    else:
        user_geolocated = False

    underage_condition = []
    if user.age and user.age < 18:
        underage_condition.append(offer_table.is_underage_recommendable)

    if user_geolocated:
        user_distance = func.ST_Distance(
            user_point,
            func.Geometry(
                func.ST_MakePoint(
                    offer_table.venue_longitude,
                    offer_table.venue_latitude,
                )
            ),
        ).label("user_distance")

        nearest_offer = (
            db.query(
                offer_table.offer_id.label("offer_id"),
                offer_table.item_id.label("item_id"),
                offer_table.venue_id.label("venue_id"),
                user_distance,
                offer_table.booking_number.label("booking_number"),
                offer_table.stock_price.label("stock_price"),
                offer_table.offer_creation_date.label("offer_creation_date"),
                offer_table.stock_beginning_date.label("stock_beginning_date"),
                offer_table.category.label("category"),
                offer_table.subcategory_id.label("subcategory_id"),
                offer_table.search_group_name.label("search_group_name"),
                offer_table.venue_latitude.label("venue_latitude"),
                offer_table.venue_longitude.label("venue_longitude"),
                offer_table.is_geolocated.label("is_geolocated"),
            )
            .filter(offer_table.item_id == item.item_id)
            .filter(offer_table.stock_price <= user.user_deposit_remaining_credit)
            .filter(
                or_(
                    offer_table.default_max_distance >= user_distance,
                    user_distance == None,
                )
            )
            .filter(*underage_condition)
            .order_by(user_distance)
            .limit(1)
            .all()
        )

        nearest_offer = [
            RecommendableOffer(
                offer_id=offer_id,
                item_id=item_id,
                venue_id=venue_id,
                user_distance=user_distance,
                booking_number=booking_number,
                stock_price=stock_price,
                offer_creation_date=offer_creation_date,
                stock_beginning_date=stock_beginning_date,
                category=category,
                subcategory_id=subcategory_id,
                search_group_name=search_group_name,
                venue_latitude=venue_latitude,
                venue_longitude=venue_longitude,
                item_score=item.item_score,
            )
            for offer_id, item_id, venue_id, user_distance, booking_number, stock_price, offer_creation_date, stock_beginning_date, category, subcategory_id, search_group_name, venue_latitude, venue_longitude, is_geolocated in nearest_offer
        ]

    else:
        nearest_offer = (
            db.query(
                offer_table.offer_id.label("offer_id"),
                offer_table.item_id.label("item_id"),
                offer_table.venue_id.label("venue_id"),
                offer_table.booking_number.label("booking_number"),
                offer_table.stock_price.label("stock_price"),
                offer_table.offer_creation_date.label("offer_creation_date"),
                offer_table.stock_beginning_date.label("stock_beginning_date"),
                offer_table.category.label("category"),
                offer_table.subcategory_id.label("subcategory_id"),
                offer_table.search_group_name.label("search_group_name"),
                offer_table.is_geolocated.label("is_geolocated"),
            )
            .filter(offer_table.item_id == item.item_id)
            .limit(1)
            .all()
        )

        nearest_offer = [
            RecommendableOffer(
                offer_id=offer_id,
                item_id=item_id,
                venue_id=venue_id,
                booking_number=booking_number,
                stock_price=stock_price,
                offer_creation_date=offer_creation_date,
                stock_beginning_date=stock_beginning_date,
                category=category,
                subcategory_id=subcategory_id,
                search_group_name=search_group_name,
                item_score=item.item_score,
            )
            for offer_id, item_id, venue_id, booking_number, stock_price, offer_creation_date, stock_beginning_date, category, subcategory_id, search_group_name, is_geolocated in nearest_offer
        ]

    return nearest_offer
