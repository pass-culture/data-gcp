import os

os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

import lancedb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from loguru import logger

ID_COLUMN = "item_id"

# Columns read from the joined BigQuery export
# (item_embedding ⋈ item_metadata ⋈ recommendable_item).
# `offer_name` / `offer_description` build the FTS `search_text` column; the
# rest mirrors `DEFAULT_DETAIL_COLUMNS` (two_tower / graph retrievals) so the
# served item payload carries the same information across all 3 retrievals,
# and `search_group_name` can be used as a filter (`params`) on every retrieval.
SOURCE_TEXT_COLUMNS = [
    "offer_name",
    "offer_description",
]
SOURCE_STRING_METADATA_COLUMNS = [
    "category",
    "subcategory_id",
    "search_group_name",
    "gtl_id",
    "gtl_l3",
    "gtl_l4",
    "example_offer_id",
    "example_offer_name",
    "example_venue_id",
]
SOURCE_PASSTHROUGH_METADATA_COLUMNS = [
    "is_geolocated",
    "booking_number",
    "booking_number_last_7_days",
    "booking_number_last_14_days",
    "booking_number_last_28_days",
    "booking_number_desc",
    "total_offers",
    "stock_price",
    # `offer_creation_date` / `stock_beginning_date` are exported as
    # unix-epoch seconds (FLOAT64) directly in the BigQuery `EXPORT DATA`
    # query (see `create_semantic_db` DAG), like `_to_ts` in the two_tower /
    # graph retrieval. Kept as a raw DATE, the value would leak as an exotic
    # string (RFC 2822 date) once it crosses the gRPC/JSON boundary, breaking
    # the recommendation API's Pydantic `datetime` parsing.
    "offer_creation_date",
    "stock_beginning_date",
    "semantic_emb_mean",
    "example_venue_latitude",
    "example_venue_longitude",
]
SOURCE_METADATA_COLUMNS = (
    SOURCE_TEXT_COLUMNS
    + SOURCE_STRING_METADATA_COLUMNS
    + SOURCE_PASSTHROUGH_METADATA_COLUMNS
)

# Final LanceDB `items` schema.
LANCEDB_COLUMNS = [
    "vector",
    "item_id",
    "item_name",
    "item_description",
    "search_text",
    *SOURCE_STRING_METADATA_COLUMNS,
    *SOURCE_PASSTHROUGH_METADATA_COLUMNS,
]


def parquet_batch_generator(
    parquet_uri: str,
    batch_size: int,
    emb_size: int,
    vector_column_name: str = "all_items_metadata_embedding",
):
    """Yield reshaped ``pyarrow.Table`` batches from a Parquet dir on GCS or local.

    Each batch is reshaped to the LanceDB ``items`` schema (``LANCEDB_COLUMNS``):
    the embedding becomes a fixed-size ``float32`` ``vector`` column, the
    ``offer_*`` metadata is renamed / cast to string, ``search_text`` (name +
    description) is derived to feed the full-text-search index, and the
    ``recommendable_item`` metadata (booking numbers, ``search_group_name``,
    gtl, example offer/venue, dates, prices/coordinates, ...) is passed
    through as-is: it is already exported in the correct type (unix-epoch
    seconds for dates, ``FLOAT64`` for BigQuery NUMERIC columns) by the
    ``EXPORT DATA`` query in the ``create_semantic_db`` DAG, so the served
    item payload matches the two_tower / graph retrievals byte-for-byte. The
    source is streamed so the full ~5M-row table is never materialised in
    memory.

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
        search_text = pc.utf8_trim_whitespace(
            pc.binary_join_element_wise(item_name, item_description, " ")
        )

        string_metadata = {
            col: pc.fill_null(pc.cast(batch.column(col), pa.string()), "")
            for col in SOURCE_STRING_METADATA_COLUMNS
        }
        # Booking numbers, dates and NUMERIC columns are passed through
        # as-is: their type is already correct in the BigQuery export (see
        # the `EXPORT DATA` query in the `create_semantic_db` DAG) and some
        # (e.g. booking counts) must keep `null` rather than being
        # zero-filled, since `null` means "no recommendable offer" downstream.
        passthrough_metadata = {
            col: batch.column(col) for col in SOURCE_PASSTHROUGH_METADATA_COLUMNS
        }

        yield pa.table(
            {
                "vector": vector,
                "item_id": item_id,
                "item_name": item_name,
                "item_description": item_description,
                "search_text": search_text,
                **string_metadata,
                **passthrough_metadata,
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
     ``language="French"`` enables French stemming/stop-words;
    - Scalar indexes: BTREE on ``item_id`` for the fast query-item vector lookup,
      BITMAP on the low-cardinality ``category`` / ``subcategory_id`` /
      ``search_group_name`` filters (used by the ``params`` filter input).
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

    logger.info("Creating native FTS index on 'search_text' (French)")
    lancedb_table.create_fts_index(
        "search_text",
        use_tantivy=False,
        language="French",
        replace=True,
    )

    logger.info(
        "Creating scalar indexes on 'item_id', 'category', 'subcategory_id', "
        "'search_group_name'"
    )
    lancedb_table.create_scalar_index("item_id", index_type="BTREE")
    lancedb_table.create_scalar_index("category", index_type="BITMAP")
    lancedb_table.create_scalar_index("subcategory_id", index_type="BITMAP")
    lancedb_table.create_scalar_index("search_group_name", index_type="BITMAP")

    logger.success(f"Table '{lancedb_table}' indexed and ready!")
