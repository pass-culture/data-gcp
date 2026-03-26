import lancedb
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger


def parquet_batch_generator(
    parquet_uri: str, batch_size: int, vector_column_name: str = "semantic_content_sts"
):
    """Yields pyarrow.RecordBatch from a Parquet directory stored on GCS or local."""
    try:
        logger.info(f"Streaming data from {parquet_uri} in batches of {batch_size}")
        dataset = ds.dataset(parquet_uri, format="parquet")
        logger.info(f"Found {len(dataset.files)} parquet files")

        for i, batch in enumerate(dataset.to_batches(batch_size=batch_size)):
            if i % 10 == 0:
                logger.info(f"Processed {i * batch_size:,} rows")
            batch = batch.rename_columns({vector_column_name: "vector"})
            table = pa.Table.from_batches([batch])
            # Drop all list columns except "vector"
            drop_cols = [
                f.name for f in table.schema if f.name not in ("vector", "item_id")
            ]
            table = table.drop(drop_cols)
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
):
    logger.info(f"Connecting to LanceDB at: {lancedb_uri}")
    db = lancedb.connect(lancedb_uri)

    logger.info(f"Checking for existing table '{lancedb_table}'")
    try:
        existing_tables = db.table_names()
        if lancedb_table in existing_tables:
            logger.info(f"Dropping existing table '{lancedb_table}'")
            db.drop_table(lancedb_table)
        else:
            logger.info("No existing table found")
    except Exception as e:
        logger.warning(f"Could not check/drop table: {e}")

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

        num_rows = table.count_rows()
        logger.success(f"Table '{lancedb_table}' created with {num_rows:,} rows")
        logger.info(f"Schema: {table.schema}")

        # Scale index parameters to dataset size:
        # - num_partitions: sqrt(num_rows) is a good heuristic, but capped at 256
        #   to avoid "too many open files" errors during index creation.
        # - num_sub_vectors: must evenly divide vector dimension
        num_partitions = min(256, max(2, int(num_rows**0.5)))
        num_sub_vectors = 48
        logger.info(
            f"Creating IVF_PQ index with cosine distance, "
            f"{num_partitions} partitions, {num_sub_vectors} sub-vectors"
        )
        table.create_index(
            vector_column_name="vector",
            metric="cosine",
            num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
            index_type="IVF_PQ",
            replace=True,
        )
        logger.success(f"Table '{lancedb_table}' indexed and ready!")

    except Exception as e:
        logger.error(f"LanceDB table creation failed: {e}")
        raise
