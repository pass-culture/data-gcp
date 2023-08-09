from dataclasses import dataclass


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
    query_order: int  # SQL query order by (lower = better)
    random: float
    offer_score: float = None  # higher = better
