from typing import List

import lancedb
import pandas as pd
import typer
from lancedb.pydantic import LanceModel, Vector
from loguru import logger
from utils.common import (
    preprocess_embeddings,
    reduce_embeddings_and_store_reducer,
)

# Define constants
from constants import (
    MODEL_TYPE,
    NUM_PARTITIONS,
    NUM_SUB_VECTORS,
    LANCEDB_BATCH_SIZE,
    UNKNOWN_PERFORMER,
    MODEL_PATH,
)

COLUMN_NAME_LIST = ["item_id", "performer"]


def preprocess_data_and_store_reducer(
    gcs_path: str, column_name_list: List[str], reducer_path: str
) -> pd.DataFrame:
    """
    Prepare the table by reading the parquet file from GCS, preprocessing embeddings,
    and merging the embeddings with the dataframe.

    Args:
        gcs_path (str): The GCS path to the parquet file.
        column_name_list (List[str]): The list of column names to read from the parquet file.

    Returns:
        pd.DataFrame: The prepared dataframe with embeddings.
    """
    item_df = pd.read_parquet(gcs_path, columns=column_name_list).assign(
        performer=lambda df: df["performer"].fillna(value=UNKNOWN_PERFORMER),
    )
    return item_df.assign(
        vector=reduce_embeddings_and_store_reducer(
            embeddings=preprocess_embeddings(gcs_path),
            n_dim=MODEL_TYPE["n_dim"],
            reducer_path=reducer_path,
        )
    )


def create_items_table(items_df: pd.DataFrame) -> None:
    """
    Create a LanceDB table from the given dataframe and create an index on it.

    Args:
        items_df (pd.DataFrame): The dataframe to create the table from.
    """

    class ItemModel(LanceModel):
        vector: Vector(32)
        item_id: str
        performer: str

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
    db.drop_database()
    db.create_table(
        "items",
        make_batches(df=items_df, batch_size=LANCEDB_BATCH_SIZE),
        schema=ItemModel,
    )
    db.open_table("items").create_index(
        num_partitions=NUM_PARTITIONS, num_sub_vectors=NUM_SUB_VECTORS
    )


def main(
    source_gcs_path: str = typer.Option(
        "gs://mlflow-bucket-prod/linkage_vector_prod/",
        help="GCS parquet path",
    ),
    input_table_name: str = typer.Option(
        "item_sources_data",
        help="GCS parquet path",
    ),
) -> None:
    """
    Main function to download and prepare the table, create the LanceDB table, and save the model type.

    Args:
        source_gcs_path (str): The GCS path to the source parquet files.
        input_table_name (str): The name of the input table.
    """
    logger.info("Download and prepare table...")
    item_df_enriched = preprocess_data_and_store_reducer(
        f"{source_gcs_path}/{input_table_name}",
        COLUMN_NAME_LIST,
        MODEL_TYPE["reducer_pickle_path"],
    )
    logger.info("Creating LanceDB table...")
    create_items_table(item_df_enriched)
    logger.info("LanceDB table and index created...")


if __name__ == "__main__":
    typer.run(main)
