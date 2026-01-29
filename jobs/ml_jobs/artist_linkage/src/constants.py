import json
import os
from pathlib import Path
from typing import ClassVar

# Infra
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"

# Mlflow
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
MLFLOW_SECRET_NAME = "mlflow_client_id"

# Config
ARTIST_LINKAGE_CONFIG = json.loads(
    Path("src/artist_linkage_config.json").read_text(encoding="utf-8")
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
WIKIDATA_ID_KEY = "wikidata_id"
ARTIST_DESCRIPTION_KEY = "artist_description"
IMG_KEY = "img"
WIKIDATA_IMAGE_FILE_URL_KEY = "wikidata_image_file_url"
WIKIDATA_IMAGE_AUTHOR_KEY = "wikidata_image_author"
WIKIDATA_IMAGE_LICENSE_KEY = "wikidata_image_license"
WIKIDATA_IMAGE_LICENSE_URL_KEY = "wikidata_image_license_url"
ACTION_KEY = "action"
COMMENT_KEY = "comment"
ARTIST_MEDIATION_UUID_KEY = "artist_mediation_uuid"
ARTIST_BIOGRAPHY_KEY = "artist_biography"
ARTIST_WIKI_ID_KEY = "artist_wiki_id"

ID_PER_CATEGORY = "id_per_category"
TOTAL_BOOKING_COUNT = "total_booking_count"
TOTAL_OFFER_COUNT = "total_offer_count"
OFFER_IS_SYNCHRONISED = "offer_is_synchronised"
FIRST_ARTIST_KEY = "first_artist"
IS_MULTI_ARTISTS_KEY = "is_multi_artists"
PREPROCESSED_ARTIST_NAME_KEY = "preprocessed_artist_name"
POSTPROCESSED_ARTIST_NAME_KEY = "postprocessed_artist_name"
WIKIPEDIA_URL_KEY = "wikipedia_url"
WIKIPEDIA_CONTENT_KEY = "wikipedia_content"

# Dataframe Columns List
ARTIST_ALIASES_KEYS = [
    ARTIST_ID_KEY,
    OFFER_CATEGORY_ID_KEY,
    ARTIST_TYPE_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    WIKIDATA_ID_KEY,
]
ARTISTS_KEYS = [
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_DESCRIPTION_KEY,
    IMG_KEY,
    WIKIDATA_ID_KEY,
    WIKIPEDIA_URL_KEY,
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


class Comment:
    linked_to_existing_artist: ClassVar[str] = "linked to existing artist"
    removed_linked: ClassVar[str] = "removed linked"
    linked_to_new_artist: ClassVar[str] = "linked to new artist"
    new_artist: ClassVar[str] = "new artist"
    new_artist_alias: ClassVar[str] = "new artist_alias"


class ProductToLinkStatus:
    not_matched_with_artists_key = "not matched with artists"
    removed_products_key = "removed product"
    matched_with_artists_key = "matched with artists"


# Robot files
WIKIMEDIA_REQUEST_HEADER = {
    "User-Agent": "PassCulture/1.0 (https://passculture.app; contact@passculture.app) Python/requests"
}  # Required to avoid 403 errors from Wikimedia API
