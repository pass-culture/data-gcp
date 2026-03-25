import lancedb
import pyarrow.dataset as ds
import typer
from lancedb.pydantic import LanceModel, Vector
from loguru import logger


class ItemEmbedding(LanceModel):
    """Schema for item embeddings in LanceDB."""

    item_id: str
    vector: Vector(768)


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
            yield batch
    except Exception as e:
        logger.error(f"Error during GCS streaming: {e}")
        raise


def build_lancedb_table(
    gcs_embedding_parquet_file: str = typer.Option(..., help="GCS Parquet file path"),
    lancedb_uri: str = typer.Option(..., help="LanceDB URI"),
    lancedb_table: str = typer.Option(..., help="LanceDB table name"),
    batch_size: int = typer.Option(..., help="Batch size for streaming"),
    vector_column_name: str = typer.Option(
        "semantic_content_sts", help="Name of the vector column in the parquet file"
    ),
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
            schema=ItemEmbedding,
            data=parquet_batch_generator(
                gcs_embedding_parquet_file,
                batch_size,
                vector_column_name=vector_column_name,
            ),
        )

        logger.success(
            f"Table '{lancedb_table}' created with {table.count_rows():,} rows"
        )
        logger.info(f"Schema: {table.schema}")

        logger.info("Creating vector index with cosine distance")
        table.create_index(
            vector_column_name="vector",
            metric="cosine",
            num_partitions=2048,
            num_sub_vectors=48,
            index_type="IVF_PQ",
            replace=True,
        )
        logger.success(f"Table '{lancedb_table}' indexed and ready!")

    except Exception as e:
        logger.error(f"LanceDB table creation failed: {e}")
        raise


if __name__ == "__main__":
    typer.run(build_lancedb_table)
