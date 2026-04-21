import os

GCP_PROJECT = os.getenv("GCP_PROJECT", "passculture-data-ehp")
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
ENVIRONMENT = "prod" if ENV_SHORT_NAME == "prod" else "ehp"
HUGGINGFACE_MODEL = "google/embeddinggemma-300m"


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
