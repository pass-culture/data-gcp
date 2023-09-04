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


def get_nearest_offers(
    db: Session, user: User, recommendable_items: List[RecommendableItem]
) -> List[Offer]:
    offer_table = get_available_table(bind_engine, "RecommendableOffersRaw")

    non_recommendable_items = get_non_recommendable_items(db, user)

    recommendable_items_ids = [
        item.item_id
        for item in recommendable_items
        if item.item_id not in non_recommendable_items
    ]

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

        offer_rank = (
            func.row_number()
            .over(partition_by=offer_table.item_id, order_by=user_distance)
            .label("offer_rank")
        )

    nearest_offers_subquery = (
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
            offer_rank,
        )
        .filter(offer_table.item_id.in_(recommendable_items_ids))
        .filter(offer_table.stock_price <= user.user_deposit_remaining_credit)
        .filter(
            or_(
                offer_table.default_max_distance >= user_distance,
                user_distance == None,
            )
        )
        .filter(*underage_condition)
        .subquery()
    )

    nearest_offers = (
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
            offer_rank,
        )
        .join(
            nearest_offers_subquery,
            offer_table.offer_id == nearest_offers_subquery.c.offer_id,
        )
        .filter(nearest_offers_subquery.c.offer_rank == 1)
        .all()
    )

    nearest_offers_result = []
    for offer in nearest_offers:
        nearest_offers_result.append(
            RecommendableOffer(
                offer_id=offer[0],
                item_id=offer[1],
                venue_id=offer[2],
                user_distance=offer[3],
                booking_number=offer[4],
                stock_price=offer[5],
                offer_creation_date=offer[6],
                stock_beginning_date=offer[7],
                category=offer[8],
                subcategory_id=offer[9],
                search_group_name=offer[10],
                venue_latitude=offer[11],
                venue_longitude=offer[12],
                item_score=[
                    reco for reco in recommendable_items if reco.item_id == offer[1]
                ][0].item_score,
            )
        )

    return nearest_offers_result
