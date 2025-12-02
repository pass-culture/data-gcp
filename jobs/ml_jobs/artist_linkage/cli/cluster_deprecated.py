# %% Deprecated but kept for reference in case we need to re-cluster the artists

import time

import numpy as np
import pandas as pd
import rapidfuzz
import typer
from loguru import logger

from src.constants import OFFER_IS_SYNCHRONISED
from src.utils.clustering_utils import (
    cluster_with_distance_matrices,
    format_cluster_matrix,
    get_cluster_to_nickname_dict,
)

app = typer.Typer()


# Parameters
NUM_CHUNKS = 50
SPARSE_FILTER_THRESHOLD = 0.2
DTYPE_DISTANCE_MATRIX = np.uint8  # np.uint8, np.uint16, np.float32
DISTANCE_METRIC = rapidfuzz.distance.OSA.normalized_distance
CLUSTERING_THRESHOLD = 0.1
RATIO_SYNCHRONISED_DATA_THRESHOLD = 0.8


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
        logger.info(
            f"Matching artists names for group {group_name} containing {len(group_df.preprocessed_artist_name.unique())} artists"
        )

        ratio_synchronised_data = (
            group_df[OFFER_IS_SYNCHRONISED].sum() / len(group_df[OFFER_IS_SYNCHRONISED])
            if len(group_df[OFFER_IS_SYNCHRONISED]) > 0
            else 0
        )
        if ratio_synchronised_data >= RATIO_SYNCHRONISED_DATA_THRESHOLD:
            # Cluster by exactly matching on preprocessed_artist_name for synchronised data
            clusters_by_group_df = (
                group_df.loc[lambda df: df[OFFER_IS_SYNCHRONISED]]
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
        logger.info("Time to compute the matching", time.time() - t0)
    clusters_df = pd.concat(clusters_df_list)

    merged_df = preprocessed_df.merge(
        clusters_df.explode("preprocessed_artist_name")[
            [
                "preprocessed_artist_name",
                "offer_category_id",
                "artist_type",
                "cluster_id",
            ]
        ],
        how="left",
        on=["preprocessed_artist_name", "offer_category_id", "artist_type"],
    ).loc[
        lambda df: df.cluster_id.notna()
    ]  # Arise from not syncrhonized offers for mostly synchronized categories

    output_df = merged_df.assign(
        artist_nickname=lambda df: df.cluster_id.map(
            get_cluster_to_nickname_dict(merged_df)
        )
    )

    output_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
