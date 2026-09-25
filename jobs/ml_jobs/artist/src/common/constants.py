import os

# Infra
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"

# Hugging Face — shared by linkage (namesake dedup embeddings) and similarity
# (biography embeddings), both loading the same encoder model.
HF_TOKEN_SECRET_NAME = (
    "huggingface_token_prod" if ENV_SHORT_NAME == "prod" else "huggingface_token_ehp"
)
ENCODER_NAME = "google/embeddinggemma-300m"

# Mlflow
MLFLOW_URI = (
    "https://mlflow.passculture.team/"
    if ENV_SHORT_NAME == "prod"
    else "https://mlflow.staging.passculture.team/"
)
MLFLOW_SECRET_NAME = "mlflow_client_id"

# Column names genuinely shared across 2+ domains — the core artist identity/join
# keys that extraction, linkage, and similarity all read or write. Domain-specific
# column names live in src/linkage/constants.py and src/similarity/constants.py.
ARTIST_ID_KEY = "artist_id"
ARTIST_NAME_KEY = "artist_name"
WIKIDATA_ID_KEY = "wikidata_id"
WIKIPEDIA_URL_KEY = "wikipedia_url"
