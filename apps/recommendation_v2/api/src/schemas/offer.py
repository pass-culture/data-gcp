from pydantic import BaseModel
from dataclasses import dataclass


class OfferInput(BaseModel):
    """Acceptable input in a API request for offer"""

    offer_id: str
    longitude: float = None
    latitude: float = None


@dataclass
class Offer:
    """Characteristics of an offer"""

    offer_id: str
    item_id: str = None
    call_id: str = None
    latitude: float = None
    longitude: float = None
    iris_id: str = None
    item_id: str = None
    cnt_bookings: float = None
