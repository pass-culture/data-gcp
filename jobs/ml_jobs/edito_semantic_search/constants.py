import os

LANCEDB_URI = os.getenv("LANCEDB_URI", "")
GCS_EMBEDDING_PARQUET_FILE = os.getenv(
    "GCS_EMBEDDING_PARQUET_FILE",
    "",
)
LANCEDB_TABLE = os.getenv("LANCEDB_TABLE", "embeddings")
