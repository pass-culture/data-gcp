import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from loguru import logger


def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default

GCP_PROJECT=os.getenv("GCP_PROJECT", "passculture-data-ehp")
ENV_SHORT_NAME=os.getenv("ENV_SHORT_NAME", "dev")
OPENAI_API_KEY=os.getenv("OPENAI_API_KEY",None)
if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY environment variable not set. Attempting to fetch from Secret Manager.")
    OPENAI_API_KEY=access_secret(GCP_PROJECT,"openai_api_key")
    logger.info(f"OPENAI_API_KEY set: {OPENAI_API_KEY != None}")
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

# --- Configuration ---
# Path where LanceDB will store its data. This will create a directory.
DB_PATH = "lancedb_parquet_rag.db" # Adjusted DB path for parquet-only setup
# Name of the table within the LanceDB database
TABLE_NAME = "my_rag_data" # Adjusted table name for parquet-only setup
# Dummy parquet file path for demonstration purposes when importing from parquet.
PARQUET_FILE = "chatbot_test_dataset_enriched.parquet" # Keeping existing dummy file name
DUMMY_PARQUET = "dummy_data.parquet" # Keeping existing dummy file name
BUCKET_NAME = f"data-bucket-ml-temp-{ENV_SHORT_NAME}"
GCS_VECTOR_DB_PATH="chatbot_edito"
# Global variables for models and API keys
SENTENCE_TRANSFORMER_MODEL = 'all-MiniLM-L6-v2' # Model for embeddings
OPENAI_LLM_MODEL = "gpt-3.5-turbo" # Or "gpt-4" for higher quality
