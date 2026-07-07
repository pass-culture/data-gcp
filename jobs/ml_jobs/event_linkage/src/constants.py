import os

# Infra
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# Hugging Face
HF_TOKEN_SECRET_NAME = (
    "huggingface_token_prod" if ENV_SHORT_NAME == "prod" else "huggingface_token_ehp"
)

# Columns
OFFER_ID_COL = "offer_id"
OFFER_NAME_COL = "offer_name"
OFFER_DESCRIPTION_COL = "offer_description"
OFFER_SUBCATEGORY_ID_COL = "offer_subcategory_id"
IMAGE_URL_COL = "image_url"
IMAGE_EMBEDDING_COL = "image_embedding"

NAME_SIMILARITY_COL = "name_similarity"
PARTIAL_NAME_SIMILARITY_COL = "partial_name_similarity"
FULL_NAME_SIMILARITY_COL = "full_name_similarity"
IMAGE_SIMILARITY_COL = "image_similarity"
DESCRIPTION_SIMILARITY_COL = "description_similarity"
FULL_DESCRIPTION_SIMILARITY_COL = "full_description_similarity"

EVENT_ID_COL = "event_id"
EVENT_SERIES_ID_COL = "event_series_id"
EVENT_NAME_COL = "event_name"
EVENT_DESCRIPTION_COL = "event_description"
EVENT_IMAGE_URL_COL = "event_image_url"

# Thresholds
MIN_DESCRIPTION_LENGTH = 150
PARTIAL_NAME_SIMILARITY_THRESHOLD = 60
NAME_SIMILARITY_THRESHOLD = 90
DESCRIPTION_SIMILARITY_THRESHOLD = 95
IMAGE_SIMILARITY_THRESHOLD = 0.8
