from sqlalchemy.orm import Session
from sqlalchemy import func

from huggy.models import enriched_user
from huggy.schemas.user import User
from huggy.crud.iris import get_iris_from_coordinates


def get_user_profile(
    db: Session, user_id: str, call_id: str, latitude: float, longitude: float
) -> User:
    """Query the database in ORM mode to get additional information about
    an user. (age, number of bookings, number of clicks, number of favorites,
    amount of remaining deposit).
    """
    user_profile = (
        db.query(
            enriched_user.User.user_deposit_creation_date
            - enriched_user.User.user_birth_date,
            func.coalesce(enriched_user.User.booking_cnt, 0),
            func.coalesce(enriched_user.User.consult_offer, 0),
            func.coalesce(enriched_user.User.has_added_offer_to_favorites, 0),
            func.coalesce(
                enriched_user.User.user_theoretical_remaining_credit,
                enriched_user.User.user_deposit_initial_amount,
            ),
        )
        .filter(enriched_user.User.user_id == user_id)
        .first()
    )

    if latitude and longitude:
        iris_id = get_iris_from_coordinates(db, latitude, longitude)

    if user_profile:

        user = User(
            user_id=user_id,
            call_id=call_id,
            longitude=longitude,
            latitude=latitude,
            found=True,
            iris_id=iris_id,
            age=int(user_profile[0].days / 365) if user_profile[0] else None,
            bookings_count=user_profile[1],
            clicks_count=user_profile[2],
            favorites_count=user_profile[3],
            user_deposit_remaining_credit=user_profile[4],
        )

        return user
