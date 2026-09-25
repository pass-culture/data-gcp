import json
from pathlib import Path
from typing import ClassVar

from src.common.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIDATA_ID_KEY,
    WIKIPEDIA_URL_KEY,
)

# Config
ARTIST_LINKAGE_CONFIG = json.loads(
    Path("src/linkage/artist_linkage_config.json").read_text(encoding="utf-8")
)
ARTIST_NAME_TO_FILTER = ARTIST_LINKAGE_CONFIG["preprocessing"]["artist_names_to_remove"]

# Column names
ID_KEY = "id"  # apparently unused anywhere in the codebase currently
PRODUCT_ID_KEY = "offer_product_id"
ARTIST_NAME_TO_MATCH_KEY = "artist_name_to_match"
ARTIST_TYPE_KEY = "artist_type"
OFFER_CATEGORY_ID_KEY = "offer_category_id"
ARTIST_DESCRIPTION_KEY = "artist_description"
IMG_KEY = "img"
SPOTIFY_ID_KEY = "spotify_id"
ISNI_ID_KEY = "isni_id"
APPLE_MUSIC_ID_KEY = "apple_music_id"
DEEZER_ID_KEY = "deezer_id"
GENIUS_ID_KEY = "genius_id"
SOUNDCLOUD_ID_KEY = "soundcloud_id"
ACTION_KEY = "action"
COMMENT_KEY = "comment"
ARTIST_WIKI_ID_KEY = "artist_wiki_id"
ARTIST_PRO_SEARCH_SCORE_KEY = "artist_pro_search_score"

ID_PER_CATEGORY = (
    "id_per_category"  # apparently unused anywhere in the codebase currently
)
TOTAL_BOOKING_COUNT = (
    "total_booking_count"  # apparently unused anywhere in the codebase currently
)
TOTAL_OFFER_COUNT = "total_offer_count"
OFFER_IS_SYNCHRONISED = "offer_is_synchronised"
FIRST_ARTIST_KEY = (
    "first_artist"  # apparently unused anywhere in the codebase currently
)
IS_MULTI_ARTISTS_KEY = (
    "is_multi_artists"  # apparently unused anywhere in the codebase currently
)
PREPROCESSED_ARTIST_NAME_KEY = (
    "preprocessed_artist_name"  # apparently unused anywhere in the codebase currently
)
POSTPROCESSED_ARTIST_NAME_KEY = "postprocessed_artist_name"
OFFER_NAME_KEY = "offer_name"

# Dataframe Columns List
MUSIC_PLATFORM_IDS_KEYS = [
    SPOTIFY_ID_KEY,
    ISNI_ID_KEY,
    APPLE_MUSIC_ID_KEY,
    DEEZER_ID_KEY,
    GENIUS_ID_KEY,
    SOUNDCLOUD_ID_KEY,
]
ARTISTS_KEYS = [
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_DESCRIPTION_KEY,
    IMG_KEY,
    WIKIDATA_ID_KEY,
    WIKIPEDIA_URL_KEY,
    SPOTIFY_ID_KEY,
    ISNI_ID_KEY,
    APPLE_MUSIC_ID_KEY,
    DEEZER_ID_KEY,
    GENIUS_ID_KEY,
    SOUNDCLOUD_ID_KEY,
]
PRODUCTS_KEYS = [
    PRODUCT_ID_KEY,
    ARTIST_ID_KEY,
    ARTIST_TYPE_KEY,
]


# Enum like classes
class Action:
    add: ClassVar[str] = "add"
    remove: ClassVar[str] = "remove"
    update: ClassVar[str] = "update"


class Comment:
    linked_to_existing_artist: ClassVar[str] = "linked to existing artist"
    removed_linked: ClassVar[str] = "removed linked"
    linked_to_new_artist: ClassVar[str] = "linked to new artist"
    new_artist: ClassVar[str] = "new artist"


class ProductToLinkStatus:
    not_matched_with_artists_key = "not matched with artists"
    removed_products_key = "removed product"
    matched_with_artists_key = "matched with artists"
