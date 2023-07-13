from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_
from geoalchemy2.functions import ST_Distance, ST_DWithin
from geoalchemy2.elements import WKTElement

from models.recommendable_offer_per_iris_shape_mv import RecommendableOfferIrisShape
from models import enriched_user
from schemas.user import User
from schemas.offer import Offer

DEFAULT_MAX_DISTANCE = 50000


def get_user_distance(db: Session, user: User, offer: Offer):

    if user.latitude is not None and user.longitude is not None:
        user_geolocated = True
        point = WKTElement(f"POINT({user.latitude} {user.longitude})")
    else:
        user_geolocated = False

    user_distance = db.query(
        RecommendableOfferIrisShape.offer_id,
        RecommendableOfferIrisShape.item_id,
        ST_Distance(point, RecommendableOfferIrisShape.venue_geo),
    ).filter(
        or_(
            and_(
                user_geolocated == True,
                RecommendableOfferIrisShape.is_geolocated == True,
                user.iris_id == RecommendableOfferIrisShape.iris_id,
                ST_DWithin(
                    point, RecommendableOfferIrisShape.venue_geo, DEFAULT_MAX_DISTANCE
                ),
            ),
            and_(
                user_geolocated == False,
                RecommendableOfferIrisShape.is_geolocated == False,
            ),
        )
    )
    return user_distance[0:3]
