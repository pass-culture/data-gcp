import os
import threading

from loguru import logger


def access_secret(project_id, secret_id, default=None):
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import secretmanager

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        logger.info(f"Accessing secret version: {name}")
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


GCP_PROJECT = os.getenv("GCP_PROJECT", "passculture-data-ehp")
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
ENVIRONMENT = "prod" if ENV_SHORT_NAME == "prod" else "ehp"
HUGGINGFACE_MODEL = "google/embeddinggemma-300m"

# --- Lazy-initialized embedding model (avoids import-time side effects) ---
_embedding_model = None
_embedding_lock = threading.Lock()


def get_embedding_model():
    """Lazily load the SentenceTransformer model on first use.

    This avoids downloading the model and authenticating with HuggingFace at
    import time, which would break tests and make startup fragile.
    """
    global _embedding_model
    if _embedding_model is not None:
        return _embedding_model

    with _embedding_lock:
        # Double-check after acquiring the lock
        if _embedding_model is not None:
            return _embedding_model

        from huggingface_hub import login
        from sentence_transformers import SentenceTransformer

        token = os.getenv("HUGGINGFACE_TOKEN", None)
        if not token:
            token = access_secret(GCP_PROJECT, f"huggingface_token_{ENVIRONMENT}")
            os.environ["HUGGINGFACE_TOKEN"] = token

        login(token=token)
        _embedding_model = SentenceTransformer(
            HUGGINGFACE_MODEL,
            trust_remote_code=True,
        )
        logger.info(f"SentenceTransformer model '{HUGGINGFACE_MODEL}' loaded.")
        return _embedding_model


DATABASE_URI = (
    f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/search_db"
)
# Update this path after running parquet_partioning.py
PARQUET_FILE = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/offers_{ENV_SHORT_NAME}_partitioned"
VECTOR_TABLE = "embeddings"
SCALAR_TABLE = "offers" if ENVIRONMENT == "prod" else f"offers_{ENV_SHORT_NAME}"
K_RETRIEVAL = 50
# K_RETRIEVAL = 250 if ENVIRONMENT == "prod" else 50
MAX_OFFERS = 3000 if ENVIRONMENT == "prod" else 50
GEMINI_MODEL_NAME = "gemini-2.5-flash-lite"
PERFORM_VECTOR_SEARCH = ENV_SHORT_NAME not in ["dev"]
