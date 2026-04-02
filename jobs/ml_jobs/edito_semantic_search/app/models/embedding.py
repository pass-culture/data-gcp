import os
import threading

from loguru import logger

from app.constants import ENVIRONMENT, GCP_PROJECT, HUGGINGFACE_MODEL

_embedding_model = None
_embedding_lock = threading.Lock()


def _access_secret(project_id, secret_id, default=None):
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
            token = _access_secret(GCP_PROJECT, f"huggingface_token_{ENVIRONMENT}")
            os.environ["HUGGINGFACE_TOKEN"] = token

        login(token=token)
        _embedding_model = SentenceTransformer(
            HUGGINGFACE_MODEL,
            trust_remote_code=True,
        )
        logger.info(f"SentenceTransformer model '{HUGGINGFACE_MODEL}' loaded.")
        return _embedding_model
