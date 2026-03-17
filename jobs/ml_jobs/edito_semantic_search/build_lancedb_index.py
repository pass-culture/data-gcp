import os

import lancedb
from constants import LANCEDB_TABLE, LANCEDB_URI
from dotenv import load_dotenv
from loguru import logger

# --- 1. Configuration ---
load_dotenv()
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
ENVIRONMENT = "prod" if ENV_SHORT_NAME == "prod" else "ehp"

# Ensure the URI starts with gs:// for LanceDB GCS implementation
GCS_DATABASE_URI = LANCEDB_URI
TABLE_NAME = LANCEDB_TABLE


def build_lancedb_vector_index(
    table: lancedb.Table,
) -> None:
    """Builds the specified index on the LanceDB table."""

    logger.info("Building IVF_PQ index on 'vector@' with 2048 partitions...")
    # v0.26.0 syntax
    table.create_index(
        vector_column_name="vector",
        metric="cosine",  # Best for Gemma 300M RAG
        num_partitions=2048,  # Optimized for ~2.5M - 3M rows
        num_sub_vectors=48,  # 768 / 16 = 48 (Optimized for Gemma dims)
        index_type="IVF_PQ",
        replace=True,
    )
    logger.info("Created vector index on 'vector' successfully.")


if __name__ == "__main__":
    # 1. Connect to GCS
    db = lancedb.connect(GCS_DATABASE_URI)
    table = db.open_table(TABLE_NAME)

    # 2. Diagnostics
    row_count = len(table)
    logger.info(f"Connected to GCS Table. Row count: {row_count}")

    if "vector" not in table.schema.names:
        logger.error(f"Column 'vector' not found. Available: {table.schema.names}")
    else:
        # 3. Maintenance (Crucial for GCS performance)
        logger.info("Starting maintenance on GCS files...")
        try:
            table.compact_files()
            table.cleanup_old_versions()
            logger.info("Maintenance complete.")
        except Exception as e:
            logger.warning(f"Maintenance failed (sometimes restricted on GCS): {e}")

        # 4. Build Index
        build_lancedb_vector_index(table=table)
    logger.info("Indexing process finished.")
