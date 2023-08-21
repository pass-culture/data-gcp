from pydantic import BaseModel
from dataclasses import dataclass


class UserInput(BaseModel):
    """Acceptable input in a API request for user"""

    user_id: str
    longitude: float = None
    latitude: float = None


@dataclass
class User:
    """Characteristics of an user"""

    user_id: str
    call_id: str = None
    longitude: float = None
    latitude: float = None
    iris_id: str = None
    age: float = None
    bookings_count: float = None
    clicks_count: float = None
    favorites_count: float = None
    user_deposit_remaining_credit: float = None
    found: bool = False
    is_geolocated: bool = None
