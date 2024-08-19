import os

import gcsfs
import joblib
import numpy as np
import pandas as pd
import polars as pl
import pyarrow.parquet as pq
from hnne import HNNE
from loguru import logger

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

item_columns = [
    "semantic_vector",
    "item_id",
    "performer" "offer_name",
]


def read_parquet_in_batches_gcs(gcs_path, batch_size):
    try:
        # Create a GCS file system object
        fs = gcsfs.GCSFileSystem()

        # Open the Parquet file from GCS
        with fs.open(gcs_path, "rb") as f:
            parquet_file = pq.ParquetFile(f)

            # Log total rows and row groups
            total_rows = parquet_file.metadata.num_rows
            num_row_groups = parquet_file.num_row_groups
            logger.info(
                f"Total rows: {total_rows}, Number of row groups: {num_row_groups}"
            )

            # Iterate over row groups and yield batches
            for i in range(num_row_groups):
                row_group = parquet_file.read_row_group(i).to_pandas()
                start_row = 0
                while start_row < len(row_group):
                    end_row = min(start_row + batch_size, len(row_group))
                    yield row_group.iloc[start_row:end_row]
                    start_row = end_row
    except Exception as e:
        logger.error(f"Failed to read Parquet file in batches: {e}")


def reduce_embeddings_and_store_reducer(embeddings: list, n_dim, reducer_path):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Reducing embeddings and storing reducer...")
    hnne = HNNE(dim=n_dim)
    reduced_embeddings = list(
        hnne.fit_transform(embeddings, dim=n_dim).astype(np.float32)
    )

    joblib.dump(hnne, reducer_path)

    return reduced_embeddings


def reduce_embeddings(embeddings: list, hnne_reducer: HNNE) -> list:
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Reducing embeddings...")
    return list(hnne_reducer.transform(embeddings).astype(np.float32))


def preprocess_embeddings_by_chunk(
    chunk: pd.DataFrame,
):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Loading embeddings...")
    data_pl = pl.from_pandas(chunk)
    embedding = np.vstack(np.vstack(data_pl.select("embedding"))[0]).astype(np.float32)
    return embedding
