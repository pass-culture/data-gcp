from typing import Optional

import lancedb
import numpy as np
import pandas as pd
import typer
from lancedb.pydantic import LanceModel, Vector
from loguru import logger
from sklearn.preprocessing import normalize

# Define constants
from constants import (
    LANCEDB_BATCH_SIZE,
    MODEL_PATH,
    MODEL_TYPE,
    NUM_PARTITIONS,
    NUM_SUB_VECTORS,
    PARQUET_BATCH_SIZE,
    SYNCHRO_SUBCATEGORIES,
)
from utils.common import (
    preprocess_embeddings_by_chunk,
    read_parquet_in_batches_gcs,
    reduce_embeddings_and_store_reducer,
)


def preprocess_data_and_store_reducer(
    chunk: pd.DataFrame, reducer_path: str, reduction: bool, linkage_type: str
) -> pd.DataFrame:
    """
    Prepare the table by reading the parquet file from GCS, preprocessing embeddings,
    and merging the embeddings with the dataframe.

    Args:
        chunk (pd.DataFrame): The dataframe to prepare.
        reducer_path (str): The path to store the reducer.
        reduction (bool): Whether to reduce the embeddings.
        linkage_type (str): Type of linkage to perform
    Returns:
        pd.DataFrame: The prepared dataframe with embeddings.
    """
    # extract_pattern = r"\b(?:Tome|tome|t|vol|episode|)\s*(\d+)\b"  # This pattern is for extracting the edition number
    # remove_pattern = r"\b(?:Tome|tome|t|vol|episode|)\s*\d+\b"  # This pattern is for removing the edition number and keyword

    # item_df = chunk.assign(
    #     performer=lambda df: df["performer"].fillna(value=UNKNOWN_PERFORMER),
    #     edition=lambda df: df["offer_name"]
    #     .str.extract(extract_pattern, expand=False)
    #     .astype(str)
    #     .fillna(value="1"),
    #     offer_name=lambda df: df["offer_name"]
    #     .str.replace(remove_pattern, "", regex=True)
    #     .str.strip(),
    # )
    item_df = chunk

    if reduction:
        item_df = item_df.assign(
            vector=reduce_embeddings_and_store_reducer(
                embeddings=preprocess_embeddings_by_chunk(chunk),
                n_dim=MODEL_TYPE["n_dim"],
                reducer_path=reducer_path,
            )
        ).drop(columns=["embedding"])
    else:
        item_df = item_df.assign(vector=list(preprocess_embeddings_by_chunk(chunk)))
    embeddings_list = item_df["vector"].tolist()

    # Convert embeddings to a NumPy array
    embeddings_array = np.array(embeddings_list)

    # Normalize the embeddings
    normalized_embeddings = normalize(embeddings_array, norm="l2")

    # Update the DataFrame with normalized embeddings
    item_df["vector"] = list(normalized_embeddings)
    if linkage_type == "product":
        item_df = item_df[item_df.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
    return item_df


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
        performer: str
        edition: str

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
    except Exception:
        logger.info("LanceDB table already exists...")
        tbl = db.open_table(linkage_type)
        tbl.add(
            make_batches(df=items_df, batch_size=LANCEDB_BATCH_SIZE),
        )


def create_index_on_items_table(linkage_type: str) -> None:
    db = lancedb.connect(MODEL_PATH)
    db.open_table(linkage_type).create_index(
        num_partitions=NUM_PARTITIONS, num_sub_vectors=NUM_SUB_VECTORS
    )


def main(
    input_path: str = typer.Option(
        default=...,
        help="GCS parquet path",
    ),
    linkage_type: str = typer.Option(
        default="product", help="Type of linkage to perform"
    ),
    reduction: str = typer.Option(
        default="true",
        help="Reduce the embeddings",
    ),
    batch_size: int = typer.Option(
        default=PARQUET_BATCH_SIZE,
        help="Batch size for reading the parquet file",
    ),
    unmatched_elements_path: Optional[str] = typer.Option(
        default=None, help="GCS parquet unmatched elements path"
    ),
) -> None:
    """
    Main function to download and prepare the table, create the LanceDB table, and save the model type.

    Args:
        input_path (str): The GCS path to the parquet file.
        reduction (str): Whether to reduce the embeddings.
        batch_size (int): The batch size for reading the parquet file.
    """
    logger.info("Download and prepare table...")
    reduction = True if reduction == "true" else False
    if linkage_type == "offer":
        unmatched_elements = pd.read_parquet(unmatched_elements_path)
    total_count = 0
    for chunk in read_parquet_in_batches_gcs(input_path, batch_size):
        if linkage_type == "offer":
            chunk = chunk.merge(
                unmatched_elements[["item_id"]], on="item_id", how="inner"
            )
        if linkage_type == "product":
            chunk = chunk[chunk.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]

        create_items_table(chunk, linkage_type)
        total_count += len(chunk)
    logger.info(f"Total rows processed: {total_count}")
    create_index_on_items_table(linkage_type)
    logger.info
    logger.info("LanceDB table and index created!")


if __name__ == "__main__":
    typer.run(main)
