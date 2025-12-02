import time
from typing import Callable, Generator, List

import numpy as np
import pandas as pd
import rapidfuzz
from loguru import logger
from scipy.sparse import csr_matrix, vstack
from sklearn.cluster import DBSCAN
from tqdm import tqdm

from src.constants import TOTAL_OFFER_COUNT


def _get_score_multiplier(dtype_distance_matrix: np.dtype) -> int:
    """
    Get the score multiplier for the distance matrix. The score multiplier is the maximum value of the distance matrix.
    It can be useful in order to compress the distance matrix.

    Args:
        dtype_distance_matrix (np.dtype): The data type of the distance matrix.

    Returns:
        int: The score multiplier for the distance matrix.
    """
    if dtype_distance_matrix == np.uint8:
        return 255
    elif dtype_distance_matrix == np.uint16:
        return 65535
    else:
        return 1


def _chunks(
    artist_list: List[str], num_chunks: int
) -> Generator[List[str], None, None]:
    """Yield successive n-sized chunks from artist_list."""
    for i in range(0, len(artist_list), num_chunks):
        yield artist_list[i : i + num_chunks]


def _compute_distance_matrix(
    artists_list: List[str],
    num_chunks: int,
    distance_metric: Callable,
    dtype_distance_matrix: np.dtype,
    sparse_filter_threshold: float,
):
    """
    Compute the distance matrix for a given list of artists.

    Args:
        artists_list (List[str]): The list of artists.
        num_chunks (int): The number of chunks to divide the artists_list into.
        distance_metric (Callable): The distance metric to use for computing the distance matrix.
        dtype_distance_matrix (np.dtype): The data type of the distance matrix.
        sparse_filter_threshold (float): The threshold for filtering out sparse values in the distance matrix.

    Returns:
        scipy.sparse.csr_matrix: The complete sparse distance matrix.

    Raises:
        ValueError: If num_chunks is less than or equal to 0.
    """

    if num_chunks <= 0:
        raise ValueError("num_chunks must be greater than 0")

    # Loop over the chunks
    sparse_matrices = []
    num_chunks = min(num_chunks, len(artists_list))
    for artists_chunk in tqdm(
        list(_chunks(artists_list, len(artists_list) // num_chunks))
    ):
        # Compute the distance matrix for the chunk
        distance_matrix = rapidfuzz.process.cdist(
            queries=artists_chunk,
            choices=artists_list,
            scorer=distance_metric,
            score_multiplier=_get_score_multiplier(dtype_distance_matrix),
            dtype=dtype_distance_matrix,
            workers=-1,  # -1 for all cores
        )

        # Create the Sparse Matrix
        distance_matrix[
            distance_matrix
            > sparse_filter_threshold * _get_score_multiplier(dtype_distance_matrix)
        ] = 0
        sparse_matrices.append(csr_matrix(distance_matrix))

    # Concatenate the sparse matrices
    complete_sparse_matrix = vstack(blocks=sparse_matrices, format="csr")

    logger.info("Memory used", complete_sparse_matrix.data.nbytes / 1024**2, "MB")

    return complete_sparse_matrix


def cluster_with_distance_matrices(
    group_artist_df: pd.DataFrame,
    num_chunks: int,
    clustering_threshold: float,
    dtype_distance_matrix: np.dtype,
    distance_metric: Callable,
    sparse_filter_threshold: float,
):
    """
    Perform clustering on a DataFrame of artists using distance matrices.

    Args:
        group_artist_df (pd.DataFrame): DataFrame containing artist data.
            Required columns: preprocessed_artist_name.
        num_chunks (int): Number of chunks to split the data into for distance matrix computation.
        clustering_threshold (float): Threshold for clustering.
        dtype_distance_matrix (np.dtype): Data type for the distance matrix.
        distance_metric (Callable): Distance metric function.
        sparse_filter_threshold (float): Threshold for filtering sparse distance matrix.

    Returns:
        pd.DataFrame: DataFrame with clustered artists.

    """
    if len(group_artist_df) == 0:
        raise ValueError("group_artist_df must not be empty")

    t0 = time.time()
    artists_list = group_artist_df.preprocessed_artist_name.drop_duplicates().tolist()

    complete_sparse_matrix = _compute_distance_matrix(
        artists_list=artists_list,
        num_chunks=num_chunks,
        distance_metric=distance_metric,
        dtype_distance_matrix=dtype_distance_matrix,
        sparse_filter_threshold=sparse_filter_threshold,
    )
    logger.info("Time to compute the distance matrix", time.time() - t0)

    # Perform clustering with DBSCAN
    t0 = time.time()
    clustering = DBSCAN(
        eps=clustering_threshold * _get_score_multiplier(dtype_distance_matrix),
        min_samples=2,
        metric="precomputed",
    )
    clustering.fit(complete_sparse_matrix)
    clusters = clustering.labels_
    logger.info("Time to compute the clustering", time.time() - t0)

    return (
        pd.DataFrame({"preprocessed_artist_name": artists_list})
        .assign(
            cluster=clusters,
        )
        .assign(
            cluster=lambda df: df.cluster.where(
                lambda s: s != -1,
                np.arange(df.cluster.max() + 1, df.cluster.max() + 1 + len(df)),
            )
        )  # Assign a new cluster id to the outliers
        .groupby("cluster")
        .agg({"preprocessed_artist_name": set})
    )


def format_cluster_matrix(
    cluster_df: pd.DataFrame, offer_category_id: str, artist_type: str
) -> pd.DataFrame:
    """
    Formats the cluster matrix by adding additional columns and sorting the dataframe.

    Args:
        cluster_df (pd.DataFrame): The cluster matrix dataframe.
            Required columns: preprocessed_artist_name.
        offer_category_id (str): The offer category ID.
        artist_type (str): The artist type.

    Returns:
        pd.DataFrame: The formatted cluster matrix dataframe.
    """
    return cluster_df.assign(
        num_artists=lambda df: df.preprocessed_artist_name.map(len),
        offer_category_id=offer_category_id,
        artist_type=artist_type,
        group_cluster_id=lambda df: df.index,
        cluster_id=lambda df: df.offer_category_id
        + "_"
        + df.artist_type
        + "_"
        + df.index.astype(str),
    ).sort_values("num_artists", ascending=False)


def get_cluster_to_nickname_dict(merged_df: pd.DataFrame) -> dict:
    """
    Returns a dictionary mapping cluster IDs to artist nicknames.

    Parameters:
        merged_df (pd.DataFrame): The merged DataFrame containing cluster information.
            Required columns: cluster_id, total_offer_count, artist_name.

    Returns:
        dict: A dictionary mapping cluster IDs to artist nicknames.
    """
    if len(merged_df) == 0:
        return {}

    return (
        merged_df.groupby("cluster_id")
        .apply(lambda df: df[TOTAL_OFFER_COUNT].idxmax())
        .reset_index(name="index_nickname")
        .merge(
            merged_df.first_artist.where(
                merged_df.is_multi_artists, merged_df.artist_name
            )
            .rename("artist_nickname")
            .to_frame(),
            left_on=["index_nickname"],
            right_index=True,
        )[["cluster_id", "artist_nickname"]]
        .set_index("cluster_id")["artist_nickname"]
        .to_dict()
    )
