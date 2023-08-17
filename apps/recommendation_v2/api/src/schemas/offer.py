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
    latitude: float = None
    longitude: float = None
    iris_id: str = None
    item_id: str = None
    item_score: float = None
    cnt_bookings: float = None
    user_distance: float = None
    venue_id: str = None
    stock_price: float = None
    offer_creation_date: str = None
    stock_creation_date: str = None
    category: str = None
    subcategory_id: str = None
    search_group_name: str = None
