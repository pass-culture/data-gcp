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
    booking_number: float = None
    user_distance: float = None
    venue_id: str = None
    stock_price: float = None
    offer_creation_date: str = None
    stock_beginning_date: str = None
    category: str = None
    subcategory_id: str = None
    search_group_name: str = None
    found: bool = None
    is_geolocated: bool = None


@dataclass
class RecommendableOffer:
    offer_id: str
    item_id: str
    venue_id: str
    user_distance: float
    booking_number: float
    category: str
    subcategory_id: str
    stock_price: float
    offer_creation_date: str
    stock_beginning_date: str
    search_group_name: str
    venue_latitude: float
    venue_longitude: float
    item_score: float  # lower = better
    query_order: int = None  # SQL query order by (lower = better)
    random: float = None
    offer_score: float = None  # higher = better
