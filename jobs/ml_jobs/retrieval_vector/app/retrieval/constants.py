from typing import List

SIMILARITY_USER_ITEM_COLUMN_NAME: str = "_user_item_dot_similarity"
SIMILARITY_ITEM_ITEM_COLUMN_NAME: str = "_item_item_dot_similarity"

DEFAULT_COLUMNS: List[str] = ["item_id"]
DEFAULT_DETAIL_COLUMNS: List[str] = [
    "topic_id",
    "cluster_id",
    "is_geolocated",
    "booking_number",
    "booking_number_last_7_days",
    "booking_number_last_14_days",
    "booking_number_last_28_days",
    "booking_number_desc",
    "semantic_emb_mean",
    "stock_price",
    "offer_creation_date",
    "stock_beginning_date",
    "category",
    "subcategory_id",
    "search_group_name",
    "gtl_id",
    "gtl_l3",
    "gtl_l4",
    "total_offers",
    "example_offer_id",
    "example_venue_id",
    "example_offer_name",
    "example_venue_latitude",
    "example_venue_longitude",
]


OUTPUT_METRIC_COLUMNS: List[str] = [
    "_distance",
    "_user_distance",
    SIMILARITY_ITEM_ITEM_COLUMN_NAME,
    SIMILARITY_USER_ITEM_COLUMN_NAME,
]
DEFAULT_ITEM_DOCS_PATH: str = "./metadata/item.docs"
DEFAULT_USER_DOCS_PATH: str = "./metadata/user.docs"
DEFAULT_LANCE_DB_URI: str = "./metadata/vector"
