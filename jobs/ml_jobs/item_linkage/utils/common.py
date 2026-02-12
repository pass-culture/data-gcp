import os

import gcsfs
import joblib
import numpy as np
import pandas as pd
import polars as pl
import pyarrow.parquet as pq
from hnne import HNNE
from loguru import logger
from tqdm import tqdm


def read_parquet_in_batches_gcs(gcs_path, batch_size):
    """
    Reads Parquet files in batches from a GCS (Google Cloud Storage) path.

    Args:
        gcs_path (str): The GCS path where the Parquet files are located.
        batch_size (int): The number of rows to yield in each batch.

    Yields:
        pandas.DataFrame: A batch of rows from the Parquet files.

    Raises:
        Exception: If there is an error reading the Parquet files.

    """
    try:
        # Create a GCS file system object
        fs = gcsfs.GCSFileSystem()

        # List all Parquet files in the directory
        file_list = fs.glob(gcs_path)

        for file_path in file_list:
            # Open the Parquet file from GCS
            with fs.open(file_path, "rb") as f:
                parquet_file = pq.ParquetFile(f)

                # Log total rows and row groups
                total_rows = parquet_file.metadata.num_rows
                num_row_groups = parquet_file.num_row_groups
                logger.info(
                    f"File: {file_path}, Total rows: {total_rows}, Number of row groups: {num_row_groups}"
                )

                # Iterate over row groups and yield batches
                for i in tqdm(range(num_row_groups)):
                    row_group = parquet_file.read_row_group(i).to_pandas()
                    start_row = 0
                    while start_row < len(row_group):
                        end_row = min(start_row + batch_size, len(row_group))
                        yield row_group.iloc[start_row:end_row]
                        start_row = end_row
    except Exception as e:
        logger.error(f"Failed to read Parquet files in batches: {e}")


def read_parquet_files_from_gcs_directory(gcs_directory_path, columns=None):
    """
    Reads and concatenates all Parquet files from a given GCS directory.

    Args:
        gcs_directory_path (str): The path to the GCS directory containing the Parquet files.
        columns (list): A list of column names to be read from the Parquet files.

    Returns:
        pandas.DataFrame: A DataFrame containing the combined data from all Parquet files.
    """

    # Create a GCS file system object
    fs = gcsfs.GCSFileSystem()

    # List all Parquet files in the directory
    file_list = fs.glob(f"{gcs_directory_path}/*.parquet")

    # Read and concatenate all Parquet files
    df_list = [pd.read_parquet(f"gs://{file}", columns=columns) for file in file_list]
    combined_df = pd.concat(df_list, ignore_index=True)

    return combined_df


def reduce_embeddings_and_store_reducer(embeddings: list, n_dim, reducer_path):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Reducing embeddings and storing reducer...")
    hnne = HNNE(n_components=n_dim)
    if os.path.exists(reducer_path):
        hnne = joblib.load(reducer_path)
        reduced_embeddings = list(hnne.transform(embeddings).astype(np.float32))
    else:
        reduced_embeddings = list(
            hnne.fit_transform(embeddings).astype(np.float32)
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
