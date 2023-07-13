from pydantic import BaseModel
from crud.iris import get_iris_from_coordinates
from crud.user import get_user_profile

from sqlalchemy.orm import Session


class UserInput(BaseModel):
    """Acceptable input in a API request for user"""

    user_id: str
    longitude: float = None
    latitude: float = None


class User:
    """Characteristics of an user"""

    def __init__(
        self,
        user_id: str,
        call_id: str = None,
        longitude: float = None,
        latitude: float = None,
        db: Session = None,
    ):
        self.id = user_id
        self.call_id = call_id
        if latitude and longitude:
            self.longitude = longitude
            self.latitude = latitude
            self.iris_id = get_iris_from_coordinates(db, longitude, latitude)
        user_profile = get_user_profile(db, self.id)
        self.age = user_profile["age"]
        self.bookings_count = user_profile["bookings_count"]
        self.clicks_count = user_profile["clicks_count"]
        self.favorites_count = user_profile["favorites_count"]
        self.user_deposit_remaining_credit = user_profile[
            "user_deposit_remaining_credit"
        ]
