import time
from typing import List

import jellyfish
import numpy as np
import pandas as pd
import rapidfuzz
import typer
from scipy.sparse import csr_matrix, vstack
from sklearn.cluster import DBSCAN
from tqdm import tqdm
from utils import read_parquet, upload_parquet

app = typer.Typer()


# Parameters
NUM_CHUNKS = 50
SPARSE_FILTER_THRESHOLD = 0.2
DTYPE_DISTANCE_MATRIX = np.uint8  # np.uint8, np.uint16, np.float32
SCORE_MULTIPLIER = (
    255
    if DTYPE_DISTANCE_MATRIX == np.uint8
    else 65535
    if DTYPE_DISTANCE_MATRIX == np.uint16
    else 1
)
DISTANCE_METRIC = rapidfuzz.distance.OSA.normalized_distance
CLUSTERING_THRESHOLD = 0.1


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def compute_distance_matrix(artists_list: List[str]):
    # Loop over the chunks
    sparse_matrices = []
    for artists_chunk in tqdm(
        list(chunks(artists_list, len(artists_list) // NUM_CHUNKS))
    ):
        # Compute the distance matrix for the chunk
        distance_matrix = rapidfuzz.process.cdist(
            queries=artists_chunk,
            choices=artists_list,
            scorer=DISTANCE_METRIC,
            score_multiplier=SCORE_MULTIPLIER,
            dtype=DTYPE_DISTANCE_MATRIX,
            workers=-1,  # -1 for all cores
        )

        # Create the Sparse Matrix
        distance_matrix[
            distance_matrix > SPARSE_FILTER_THRESHOLD * SCORE_MULTIPLIER
        ] = 0
        sparse_matrices.append(csr_matrix(distance_matrix))

    # Concatenate the sparse matrices
    complete_sparse_matrix = vstack(blocks=sparse_matrices, format="csr")

    print("Memory used", complete_sparse_matrix.data.nbytes / 1024**2, "MB")

    return complete_sparse_matrix


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    preprocessed_df = read_parquet(source_file_path)

    # Build the clusters
    clusters_df_list = []
    for group, group_df in preprocessed_df.groupby(
        ["offer_category_id", "artist_type"]
    ):
        artists_list = group_df.preprocessed_artist_name.drop_duplicates().tolist()

        t0 = time.time()
        print(
            f"Computing the distance for {group} containing {len(artists_list)} preprocessed artists"
        )
        complete_sparse_matrix = compute_distance_matrix(artists_list)
        print("Time to compute the ditance matrix", time.time() - t0)

        # Perform clustering with DBSCAN
        t0 = time.time()
        clustering = DBSCAN(
            eps=CLUSTERING_THRESHOLD * SCORE_MULTIPLIER,
            min_samples=2,
            metric="precomputed",
        )
        clustering.fit(complete_sparse_matrix)
        clusters = clustering.labels_

        clusters_df_list.append(
            pd.DataFrame({"artist": artists_list})
            .assign(
                artist_encoding=lambda df: df.artist.map(jellyfish.metaphone),
                cluster=clusters,
            )
            .groupby("cluster")
            .agg({"artist": set, "artist_encoding": set})
            .assign(
                num_artists=lambda df: df.artist.map(len),
                num_encodings=lambda df: df.artist_encoding.map(len),
                offer_category_id=group[0],
                artist_type=group[1],
                group_cluster_id=lambda df: df.index,
                cluster_id=lambda df: df.offer_category_id
                + "_"
                + df.artist_type
                + "_"
                + df.index.astype(str),
            )
            .sort_values("num_artists", ascending=False)
        )
        print("Time to compute the clustering", time.time() - t0)

    clusters_df = pd.concat(clusters_df_list)

    merged_df = preprocessed_df.merge(
        clusters_df.loc[lambda df: df.group_cluster_id != -1].explode("artist")[
            ["artist", "offer_category_id", "artist_type", "cluster_id"]
        ],
        how="left",
        left_on=["preprocessed_artist_name", "offer_category_id", "artist_type"],
        right_on=["artist", "offer_category_id", "artist_type"],
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
