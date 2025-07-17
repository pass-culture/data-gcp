import json
from pathlib import Path

# Config
ARTIST_LINKAGE_CONFIG = json.loads(
    Path("artist_linkage_config.json").read_text(encoding="utf-8")
)
ARTIST_NAME_TO_FILTER = ARTIST_LINKAGE_CONFIG["preprocessing"]["artist_names_to_remove"]

# Column names
ARTIST_ID_KEY = "artist_id"
ID_KEY = "id"
PRODUCT_ID_KEY = "offer_product_id"
ARTIST_NAME_KEY = "artist_name"
ARTIST_NAME_TO_MATCH_KEY = "artist_name_to_match"
ARTIST_TYPE_KEY = "artist_type"
OFFER_CATEGORY_ID_KEY = "offer_category_id"
WIKI_ID_KEY = "wiki_id"
DESCRIPTION_KEY = "description"
IMG_KEY = "img"

ID_PER_CATEGORY = "id_per_category"
TOTAL_BOOKING_COUNT = "total_booking_count"
TOTAL_OFFER_COUNT = "total_offer_count"
OFFER_IS_SYNCHRONISED = "offer_is_synchronised"
FIRST_ARTIST_KEY = "first_artist"
IS_MULTI_ARTISTS_KEY = "is_multi_artists"
PREPROCESSED_ARTIST_NAME_KEY = "preprocessed_artist_name"

# Dataframe Columns List

ARTIST_ALIASES_KEYS = [
    ARTIST_ID_KEY,
    OFFER_CATEGORY_ID_KEY,
    ARTIST_TYPE_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    WIKI_ID_KEY,
]
ARTISTS_KEYS = [ARTIST_ID_KEY, ARTIST_NAME_KEY, DESCRIPTION_KEY, IMG_KEY, WIKI_ID_KEY]
PRODUCTS_KEYS = [
    PRODUCT_ID_KEY,
    ARTIST_ID_KEY,
    ARTIST_TYPE_KEY,
]
