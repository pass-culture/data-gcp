import os

from dotenv import load_dotenv

load_dotenv()
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from huggingface_hub import login
from langchain_huggingface import HuggingFaceEmbeddings
from loguru import logger


def access_secret(project_id, secret_id, default=None):
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
HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN", None)
if not HUGGINGFACE_TOKEN:
    HUGGINGFACE_TOKEN = access_secret(GCP_PROJECT, f"huggingface_token_{ENVIRONMENT}")
    os.environ["HUGGINGFACE_TOKEN"] = HUGGINGFACE_TOKEN
HUGGINGFACE_MODEL = "google/embeddinggemma-300m"
login(token=HUGGINGFACE_TOKEN)
embedding_model = HuggingFaceEmbeddings(
    model=HUGGINGFACE_MODEL,
    query_encode_kwargs={"prompt_name": "query"},
)
DATABASE_URI = (
    f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/search_db"
)
# Update this path after running parquet_partioning.py
PARQUET_FILE = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/chatbot_encoded_offers_metadata_{ENV_SHORT_NAME}/partitioned"
VECTOR_TABLE = "embeddings"
SCALAR_TABLE = "offers" if ENVIRONMENT == "prod" else f"offers_{ENV_SHORT_NAME}"
K_RETRIEVAL = 50
# K_RETRIEVAL = 250 if ENVIRONMENT == "prod" else 50
MAX_OFFERS = 3000 if ENVIRONMENT == "prod" else 50
GEMINI_MODEL_NAME = "gemini-2.0-flash"
PERFORM_VECTOR_SEARCH = ENV_SHORT_NAME not in ["dev"]
