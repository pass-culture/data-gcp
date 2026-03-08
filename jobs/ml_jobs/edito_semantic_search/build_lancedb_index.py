import os
import lancedb
from loguru import logger
from dotenv import load_dotenv

# --- 1. Configuration ---
load_dotenv()
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
ENVIRONMENT = "prod" if ENV_SHORT_NAME == "prod" else "ehp"

# Ensure the URI starts with gs:// for LanceDB GCS implementation
GCS_DATABASE_URI = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/search_db"
TABLE_NAME = "embeddings"

def build_lancedb_index(
    table: lancedb.Table,
    index_type: str = "vector",
    index_column: str = "vector",
) -> None:
    """Builds the specified index on the LanceDB table."""
    
    if index_type == "text":
        # FTS index doesn't use IVF_PQ, it's for keyword search
        table.create_fts_index(index_column, replace=True)
        logger.info(f"Created FTS (text) index on column '{index_column}'")

    elif index_type == "vector":
        logger.info(f"Building IVF_PQ index on '{index_column}' with 2048 partitions...")
        # v0.26.0 syntax
        table.create_index(
            vector_column_name=index_column,
            metric="cosine",      # Best for Gemma 300M RAG
            num_partitions=2048,  # Optimized for ~2.5M - 3M rows
            num_sub_vectors=48,   # 768 / 16 = 48 (Optimized for Gemma dims)
            index_type="IVF_PQ",
            replace=True
        )
        logger.info(f"Created vector index on '{index_column}' successfully.")

    elif index_type == "scalar":
        table.create_scalar_index(index_column)
        logger.info(f"Created scalar index on '{index_column}'.")

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
            # Compacting helps LanceDB 'see' all 2.5M rows as large contiguous files
            table.compact_files()
            table.cleanup_old_versions()
            logger.info("Maintenance complete.")
        except Exception as e:
            logger.warning(f"Maintenance failed (sometimes restricted on GCS): {e}")

        # 4. Build Index
        build_lancedb_index(
            table=table,
            index_type="vector",
            index_column="vector"
        )
    logger.info("Indexing process finished.")
    logger.info("check index status with: lancedb.connect(GCS_DATABASE_URI).open_table(TABLE_NAME).index_status()")
    stats = table.index_stats("vector")
    logger.info(f"Unindexed rows: {stats.num_unindexed_rows}")