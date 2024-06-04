import time
from typing import Callable, Generator, List

import numpy as np
import pandas as pd
import rapidfuzz
import typer
from scipy.sparse import csr_matrix, vstack
from sklearn.cluster import DBSCAN
from tqdm import tqdm

from utils.gcs_utils import upload_parquet

app = typer.Typer()


# Parameters
NUM_CHUNKS = 50
SPARSE_FILTER_THRESHOLD = 0.2
DTYPE_DISTANCE_MATRIX = np.uint8  # np.uint8, np.uint16, np.float32
DISTANCE_METRIC = rapidfuzz.distance.OSA.normalized_distance
CLUSTERING_THRESHOLD = 0.1
RATIO_SYNCHRONISED_DATA_THRESHOLD = 0.8


def _get_score_multiplier(dtype_distance_matrix: np.dtype) -> int:
    """
    Get the score multiplier for the distance matrix. The score multiplier is the maximum value of the distance matrix.
    It can be useful in order to compress the distance matrix
    Args:
        dtype_distance_matrix: The data type of the distance matrix.
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
    if num_chunks <= 0:
        raise ValueError("num_chunks must be greater than 0")

    # Loop over the chunks
    sparse_matrices = []
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

    print("Memory used", complete_sparse_matrix.data.nbytes / 1024**2, "MB")

    return complete_sparse_matrix


def cluster_with_distance_matrices(
    group_artist_df: pd.DataFrame,
    num_chunks: int,
    clustering_threshold: float,
    dtype_distance_matrix: np.dtype,
    distance_metric: Callable,
    sparse_filter_threshold: float,
):
    artists_list = group_artist_df.preprocessed_artist_name.drop_duplicates().tolist()
    t0 = time.time()

    complete_sparse_matrix = _compute_distance_matrix(
        artists_list=artists_list,
        num_chunks=num_chunks,
        distance_metric=distance_metric,
        dtype_distance_matrix=dtype_distance_matrix,
        sparse_filter_threshold=sparse_filter_threshold,
    )
    print("Time to compute the ditance matrix", time.time() - t0)

    # Perform clustering with DBSCAN
    t0 = time.time()
    clustering = DBSCAN(
        eps=clustering_threshold * _get_score_multiplier(dtype_distance_matrix),
        min_samples=2,
        metric="precomputed",
    )
    clustering.fit(complete_sparse_matrix)
    clusters = clustering.labels_
    print("Time to compute the clustering", time.time() - t0)

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


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    preprocessed_df = pd.read_parquet(source_file_path).reset_index(drop=True)

    # Build the clusters
    clusters_df_list = []
    for group_name, group_df in preprocessed_df.groupby(
        ["offer_category_id", "artist_type"]
    ):
        t0 = time.time()
        print(
            f"Matching artists names for group {group_name} containing {len(group_df.preprocessed_artist_name.unique())} artists"
        )

        ratio_synchronised_data = group_df.is_synchronised.sum() / len(
            group_df.is_synchronised
        )
        if ratio_synchronised_data >= RATIO_SYNCHRONISED_DATA_THRESHOLD:
            clusters_by_group_df = (
                group_df.loc[lambda df: df.is_synchronised]
                .groupby("preprocessed_artist_name")
                .apply(lambda g: set(g.preprocessed_artist_name))
                .rename("preprocessed_artist_name")
                .to_frame()
                .reset_index(drop=True)
            )

        else:
            clusters_by_group_df = cluster_with_distance_matrices(
                group_artist_df=group_df,
                num_chunks=NUM_CHUNKS,
                clustering_threshold=CLUSTERING_THRESHOLD,
                dtype_distance_matrix=DTYPE_DISTANCE_MATRIX,
                distance_metric=DISTANCE_METRIC,
                sparse_filter_threshold=SPARSE_FILTER_THRESHOLD,
            )

        clusters_df_list.append(
            clusters_by_group_df.pipe(
                format_cluster_matrix,
                offer_category_id=group_name[0],
                artist_type=group_name[1],
            )
        )
        print("Time to compute the matching", time.time() - t0)

    clusters_df = pd.concat(clusters_df_list)

    merged_df = preprocessed_df.merge(
        clusters_df.loc[lambda df: df.group_cluster_id != -1].explode(
            "preprocessed_artist_name"
        )[
            [
                "preprocessed_artist_name",
                "offer_category_id",
                "artist_type",
                "cluster_id",
            ]
        ],
        how="left",
        on=["preprocessed_artist_name", "offer_category_id", "artist_type"],
    )

    cluster_to_nickname_dict = (
        (
            merged_df.groupby("cluster_id")
            .apply(lambda df: df["offer_number"].idxmax())
            .reset_index(name="index_nickname")
        )
        .merge(
            merged_df[["artist_name"]],
            left_on=["index_nickname"],
            right_index=True,
        )[["cluster_id", "artist_name"]]
        .rename(columns={"artist_name": "artist_nickname"})
        .set_index("cluster_id")["artist_nickname"]
        .to_dict()
    )

    output_df = merged_df.assign(
        artist_nickname=lambda df: df.cluster_id.map(cluster_to_nickname_dict)
    )

    upload_parquet(
        dataframe=output_df,
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()
