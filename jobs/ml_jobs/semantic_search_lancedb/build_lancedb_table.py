import lancedb
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger

ID_COLUMN = "item_id"


def parquet_batch_generator(
    parquet_uri: str, batch_size: int, vector_column_name: str = "semantic_content_sts"
):
    """Yields pyarrow.RecordBatch from a Parquet directory stored on GCS or local."""
    try:
        logger.info(f"Streaming data from {parquet_uri} in batches of {batch_size}")
        dataset = ds.dataset(parquet_uri, format="parquet")
        logger.info(f"Found {len(dataset.files)} parquet files")

        for i, batch in enumerate(
            dataset.to_batches(
                batch_size=batch_size, columns=[ID_COLUMN, vector_column_name]
            )
        ):
            if i % 10 == 0:
                logger.info(f"Processed {i * batch_size:,} rows")
            batch = batch.rename_columns({vector_column_name: "vector"})
            table = pa.Table.from_batches([batch])

            # Re-cast list columns so child field is named "item" (LanceDB default)
            # instead of "element" (parquet default), avoiding schema mismatch.
            new_fields = []
            for field in table.schema:
                if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
                    new_fields.append(field.with_type(pa.list_(field.type.value_type)))
                else:
                    new_fields.append(field)
            table = table.cast(pa.schema(new_fields))
            yield table
    except Exception as e:
        logger.error(f"Error during GCS streaming: {e}")
        raise


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

    try:
        logger.info(
            f"Creating LanceDB table '{lancedb_table}' with batch size {batch_size}"
        )

        table = db.create_table(
            name=lancedb_table,
            data=parquet_batch_generator(
                gcs_embedding_parquet_file,
                batch_size,
                vector_column_name=vector_column_name,
            ),
        )
        logger.info(f"Table '{lancedb_table}' created. Table Schema: {table.schema}")
        return table

    except Exception as e:
        logger.error(f"LanceDB table creation failed: {e}")
        raise


def create_index(lancedb_table: lancedb.Table) -> None:
    """Create LanceDB index given a table

    - num_partitions: sqrt(num_rows) is a good heuristic, but capped at 256
      to avoid "too many open files" errors during index creation.
    - num_sub_vectors: must evenly divide vector dimension

    see doc here https://docs.lancedb.com/indexing/vector-index

    """
    num_rows = lancedb_table.count_rows()
    vector_dim = lancedb_table.schema.field("vector").type.list_size

    num_partitions = min(256, max(2, int(num_rows**0.5)))
    num_sub_vectors = vector_dim // 16  # for faster retrieval, lower recall

    logger.info(
        f"Creating IVF_PQ index with cosine distance, "
        f"{num_partitions} partitions, {num_sub_vectors} sub-vectors, vector dimension {vector_dim}"
    )
    lancedb_table.create_index(
        vector_column_name="vector",
        metric="cosine",
        num_partitions=num_partitions,
        num_sub_vectors=num_sub_vectors,
        index_type="IVF_PQ",
        replace=True,
    )
    logger.success(f"Table '{lancedb_table}' indexed and ready!")
