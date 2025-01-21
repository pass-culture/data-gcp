from typing import Optional

import lancedb
import pandas as pd
import typer
from lancedb.pydantic import LanceModel, Vector
from loguru import logger

from constants import (
    LANCEDB_BATCH_SIZE,
    MODEL_PATH,
    NUM_PARTITIONS,
    NUM_SUB_VECTORS,
    PARQUET_BATCH_SIZE,
    RETRIEVAL_FILTERS,
)
from utils.common import (
    read_parquet_in_batches_gcs,
)


def create_items_table(items_df: pd.DataFrame, linkage_type: str) -> None:
    """
    Create a LanceDB table from the given dataframe and create an index on it.

    Args:
        items_df (pd.DataFrame): The dataframe to create the table from.
    """

    class ItemModel(LanceModel):
        vector: Vector(32)
        item_id: str
        offer_subcategory_id: str
        performer: Optional[str]
        edition: Optional[str]

    def make_batches(df: pd.DataFrame, batch_size: int):
        """
        Yield successive batches of the dataframe.

        Args:
            df (pd.DataFrame): The dataframe to be batched.
            batch_size (int): The size of each batch. Default is 5000.

        Yields:
            pd.DataFrame: A batch of the dataframe.
        """
        for i in range(0, len(df), batch_size):
            yield df[i : i + batch_size]

    db = lancedb.connect(MODEL_PATH)
    try:
        logger.info("Creating LanceDB table...")
        db.create_table(
            linkage_type,
            make_batches(df=items_df, batch_size=LANCEDB_BATCH_SIZE),
            schema=ItemModel,
        )
        logger.info("LanceDB table created!")
    except Exception:
        logger.info("LanceDB table already exists...")
        tbl = db.open_table(linkage_type)
        logger.info("Inserting data into LanceDB table...")
        tbl.add(
            make_batches(df=items_df, batch_size=LANCEDB_BATCH_SIZE),
        )


def create_index_on_items_table(linkage_type: str) -> None:
    db = lancedb.connect(MODEL_PATH)
    table = db.open_table(linkage_type)
    logger.info(f"Creating index on LanceDB table {len(table)}...")
    table.create_index(
        index_type="IVF_PQ",
        num_partitions=NUM_PARTITIONS,
        num_sub_vectors=NUM_SUB_VECTORS,
    )

    for feature in RETRIEVAL_FILTERS:
        logger.info(f"Creating index on feature: {feature}")
        table.create_scalar_index(feature, index_type="BITMAP")


def main(
    input_path: str = typer.Option(
        default=...,
        help="GCS parquet path",
    ),
    linkage_type: str = typer.Option(
        default="product", help="Type of linkage to perform"
    ),
    batch_size: int = typer.Option(
        default=PARQUET_BATCH_SIZE,
        help="Batch size for reading the parquet file",
    ),
) -> None:
    """
    Create the LanceDB table

    This function:
      1) Downloads and prepares the table from the specified input path.
      2) Creates the LanceDB table with the specified linkage type.

    Args:
        input_path (str): The GCS path to the parquet file.
        linkage_type (str): The type of linkage to perform.
        batch_size (int): The batch size for reading the parquet file.
    """
    logger.info("Download and prepare table...")
    total_count = 0
    for chunk in read_parquet_in_batches_gcs(input_path, batch_size):
        chunk = chunk[
            ["item_id", "vector", "offer_subcategory_id", "performer", "edition"]
        ]
        chunk = chunk.fillna("")
        logger.info(f"Processing chunk of length: {len(chunk)}")
        logger.info(f"chunk columns: {chunk.columns}")
        create_items_table(chunk, linkage_type)
        total_count += len(chunk)
    logger.info(f"Total rows processed: {total_count}")
    create_index_on_items_table(linkage_type)
    logger.info
    logger.info("LanceDB table and index created!")


if __name__ == "__main__":
    typer.run(main)
