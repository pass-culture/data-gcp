import os

LANCEDB_URI = os.getenv("LANCEDB_URI", "gs://mlflow-bucket-ehp/edito_semantic_search")
GCS_EMBEDDING_PARQUET_FILE = os.getenv(
    "GCS_EMBEDDING_PARQUET_FILE",
    "",
)
LANCEDB_TABLE = os.getenv("LANCEDB_TABLE", "embeddings")
