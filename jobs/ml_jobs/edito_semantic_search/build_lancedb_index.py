import lancedb
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from loguru import logger
# --- 1. Configuration ---
# Replace with your actual GCS URIs
from constants import GCS_DATABASE_URI, GCS_PARQUET_FILE, TABLE_NAME

def build_lancedb_index(database_uri: str, table_name: str , index_type: str = "text", index_column:str = "item_id") -> None:
    """Builds and returns a LanceDB table from the specified GCS URI."""
    db = lancedb.connect(database_uri)
    table = db.open_table(table_name)
    logger.info(f"Building {index_type} index on column '{index_column}' for table '{table_name}' in {database_uri}...")
    if index_type == "text":
        table.create_fts_index(index_column)
        logger.info(f"Created text index on column '{index_column}' with IVF_PQ method.")
    if index_type == "vector":
        table.create_vector_index(
            column=index_column,
            method="IVF_PQ",
            metric="L2",
            num_partitions=256,
            num_sub_vectors=16,
            bits_per_code=8
        )
        logger.info(f"Created vector index on column '{index_column}' with IVF_PQ method.")
    if index_type =="scalar":
        table.create_scalar_index(index_column)
        logger.info(f"Created scalar index on column '{index_column}'.")
    logger.info(f"Opened LanceDB table '{table_name}' from GCS: {database_uri}")
    return

if __name__ == "__main__":
    build_lancedb_index(
        database_uri=GCS_DATABASE_URI,
        table_name=TABLE_NAME,
        index_type="scalar",
        index_column="item_id"
    )
