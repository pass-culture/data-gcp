import os

# Infra
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# Hugging Face
HF_TOKEN_SECRET_NAME = (
    "huggingface_token_prod" if ENV_SHORT_NAME == "prod" else "huggingface_token_ehp"
)

# Columns
OFFER_ID_COLUMN = "offer_id"
OFFER_NAME_COL = "offer_name"
OFFER_DESCRIPTION_COL = "offer_description"
OFFER_SUBCATEGORY_ID_COL = "offer_subcategory_id"
IMAGE_URL_COLUMN = "image_url"
IMAGE_EMBEDDING_COLUMN = "image_embedding"
