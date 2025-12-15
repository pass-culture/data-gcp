import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from huggingface_hub import login
from langchain_huggingface import HuggingFaceEmbeddings
from loguru import logger
def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        logger.info(f"Accessing secret version: {name}")
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
GCP_PROJECT = os.getenv("GCP_PROJECT", "passculture-data-prod")
HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN", None)
if not HUGGINGFACE_TOKEN:
    HUGGINGFACE_TOKEN = access_secret(GCP_PROJECT, "huggingface_token_prod")
    os.environ["HUGGINGFACE_TOKEN"] = HUGGINGFACE_TOKEN
HUGGINGFACE_MODEL = "google/embeddinggemma-300m"
login(token=HUGGINGFACE_TOKEN)
embedding_model = HuggingFaceEmbeddings(
    model=HUGGINGFACE_MODEL,
    query_encode_kwargs={"prompt_name": "query"},
)
DATABASE_URI = "gs://mlflow-bucket-prod/streamlit_data/chatbot_edito/search_db"
VECTOR_TABLE = "embeddings"
SCALAR_TABLE = "offers"
K_RETRIEVAL=10

GEMINI_MODEL_NAME="gemini-2.5-flash"