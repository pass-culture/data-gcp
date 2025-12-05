from enum import StrEnum
from typing import List

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
    "artist_id_list",
    "series_id",
]

SIMILARITY_USER_ITEM_COLUMN_NAME: str = "_user_item_dot_similarity"
SIMILARITY_ITEM_ITEM_COLUMN_NAME: str = "_item_item_dot_similarity"
SEARCH_TYPE_COLUMN_NAME: str = "_search_type"
DISTANCE_COLUMN_NAME: str = "_distance"
USER_DISTANCE_COLUMN_NAME: str = "_user_distance"

OUTPUT_METRIC_COLUMNS: List[str] = [
    DISTANCE_COLUMN_NAME,
    USER_DISTANCE_COLUMN_NAME,
    SIMILARITY_ITEM_ITEM_COLUMN_NAME,
    SIMILARITY_USER_ITEM_COLUMN_NAME,
    SEARCH_TYPE_COLUMN_NAME,
]
DEFAULT_ITEM_DOCS_PATH: str = "./metadata/item.docs"
DEFAULT_USER_DOCS_PATH: str = "./metadata/user.docs"
DEFAULT_LANCE_DB_URI: str = "./metadata/vector"


class EmbeddingModelTypes(StrEnum):
    TWO_TOWER = "two_tower"
    SEMANTIC = "semantic"
    METADATA_GRAPH = "metadata_graph"
