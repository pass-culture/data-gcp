import lancedb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from loguru import logger

ID_COLUMN = "item_id"

# Columns read from the joined BigQuery export (item_embedding ⋈ item_metadata).
# The metadata columns power the full-text / hybrid search and the served payload.
SOURCE_METADATA_COLUMNS = [
    "offer_name",
    "offer_description",
    "offer_category_id",
    "offer_subcategory_id",
]

# Final LanceDB `items` schema.
LANCEDB_COLUMNS = [
    "vector",
    "item_id",
    "item_name",
    "item_description",
    "search_text",
    "category",
    "subcategory_id",
]


def parquet_batch_generator(
    parquet_uri: str,
    batch_size: int,
    emb_size: int,
    vector_column_name: str = "semantic_content",
):
    """Yield reshaped ``pyarrow.Table`` batches from a Parquet dir on GCS or local.

    Each batch is reshaped to the LanceDB ``items`` schema (``LANCEDB_COLUMNS``):
    the embedding becomes a fixed-size ``float32`` ``vector`` column, the
    ``offer_*`` metadata is renamed / cast to string, and ``search_text`` (name +
    description) is derived to feed the full-text-search index. The source is
    streamed so the full ~5M-row table is never materialised in memory.

    Args:
        parquet_uri: Path (GCS or local) to the parquet dir.
        batch_size: Number of rows per streamed batch.
        emb_size: Dimensionality of the embedding vectors.
        vector_column_name: Name of the embedding column in the source parquet.
    """
    columns = [ID_COLUMN, vector_column_name, *SOURCE_METADATA_COLUMNS]
    logger.info(f"Streaming data from {parquet_uri} in batches of {batch_size:,}")
    dataset = ds.dataset(parquet_uri, format="parquet")
    logger.info(f"Found {len(dataset.files)} parquet file(s)")

    for i, batch in enumerate(
        dataset.to_batches(batch_size=batch_size, columns=columns)
    ):
        if i % 10 == 0:
            logger.info(f"Processed {i * batch_size:,} rows")

        vector = pc.cast(
            batch.column(vector_column_name), pa.list_(pa.float32(), emb_size)
        )
        item_id = pc.cast(batch.column(ID_COLUMN), pa.string())
        item_name = pc.fill_null(pc.cast(batch.column("offer_name"), pa.string()), "")
        item_description = pc.fill_null(
            pc.cast(batch.column("offer_description"), pa.string()), ""
        )
        category = pc.fill_null(
            pc.cast(batch.column("offer_category_id"), pa.string()), ""
        )
        subcategory_id = pc.fill_null(
            pc.cast(batch.column("offer_subcategory_id"), pa.string()), ""
        )
        search_text = pc.utf8_trim_whitespace(
            pc.binary_join_element_wise(item_name, item_description, " ")
        )

        yield pa.table(
            {
                "vector": vector,
                "item_id": item_id,
                "item_name": item_name,
                "item_description": item_description,
                "search_text": search_text,
                "category": category,
                "subcategory_id": subcategory_id,
            }
        )


def _detect_embedding_dimension(parquet_uri: str, vector_column_name: str) -> int:
    """Peek a single row to size the fixed-length vector column."""
    dataset = ds.dataset(parquet_uri, format="parquet")
    first_batch = next(
        dataset.to_batches(batch_size=1, columns=[vector_column_name]), None
    )
    if first_batch is None or first_batch.num_rows == 0:
        raise ValueError("Cannot build a LanceDB table from an empty dataset.")
    emb_size = len(first_batch.column(vector_column_name)[0])
    logger.info(f"Detected embedding dimension: {emb_size}")
    return emb_size


def build_lancedb_table(
    gcs_embedding_parquet_file: str,
    lancedb_uri: str,
    lancedb_table: str,
    batch_size: int,
    vector_column_name: str,
) -> lancedb.Table:
    logger.info(f"Connecting to LanceDB at: {lancedb_uri}")
    db = lancedb.connect(lancedb_uri)

    existing_tables = db.table_names()
    if existing_tables:
        logger.info(
            f"Dropping {len(existing_tables)} existing table(s): {existing_tables}"
        )
        for table_name in existing_tables:
            db.drop_table(table_name)

    emb_size = _detect_embedding_dimension(
        gcs_embedding_parquet_file, vector_column_name
    )

    try:
        logger.info(
            f"Creating LanceDB table '{lancedb_table}' with batch size {batch_size}"
        )
        table = db.create_table(
            name=lancedb_table,
            data=parquet_batch_generator(
                gcs_embedding_parquet_file,
                batch_size,
                emb_size=emb_size,
                vector_column_name=vector_column_name,
            ),
        )
        logger.info(f"Table '{lancedb_table}' created. Table Schema: {table.schema}")
        return table

    except Exception as e:
        logger.error(f"LanceDB table creation failed: {e}")
        raise


def create_index(lancedb_table: lancedb.Table) -> None:
    """Create the vector, full-text and scalar indexes on the `items` table.

    - Vector (IVF_PQ, cosine): similar-item and semantic-text search.
      ``num_partitions`` ≈ sqrt(num_rows) capped at 256 (avoids "too many open
      files"); ``num_sub_vectors`` must evenly divide the vector dimension.
    - FTS on ``search_text``: keyword search and the FTS side of hybrid search.
      ``use_tantivy=False`` uses the native (Lance) FTS index, which is safe on
      object storage.
    - Scalar indexes: BTREE on ``item_id`` for the fast query-item vector lookup,
      BITMAP on the low-cardinality ``category`` / ``subcategory_id`` filters.

    see doc here https://docs.lancedb.com/indexing/vector-index
    """
    num_rows = lancedb_table.count_rows()
    vector_dim = lancedb_table.schema.field("vector").type.list_size

    num_partitions = min(256, max(2, int(num_rows**0.5)))
    num_sub_vectors = vector_dim // 16  # for faster retrieval, lower recall

    logger.info(
        f"Creating IVF_PQ index with cosine distance, "
        f"{num_partitions} partitions, {num_sub_vectors} sub-vectors, "
        f"vector dimension {vector_dim}. This may take a few minutes."
    )
    lancedb_table.create_index(
        vector_column_name="vector",
        metric="cosine",
        num_partitions=num_partitions,
        num_sub_vectors=num_sub_vectors,
        index_type="IVF_PQ",
        replace=True,
    )

    logger.info("Creating native FTS index on 'search_text'")
    lancedb_table.create_fts_index("search_text", use_tantivy=False, replace=True)

    logger.info("Creating scalar indexes on 'item_id', 'category', 'subcategory_id'")
    lancedb_table.create_scalar_index("item_id", index_type="BTREE")
    lancedb_table.create_scalar_index("category", index_type="BITMAP")
    lancedb_table.create_scalar_index("subcategory_id", index_type="BITMAP")

    logger.success(f"Table '{lancedb_table}' indexed and ready!")
