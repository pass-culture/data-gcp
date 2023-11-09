import typer
import pandas as pd
import polars as pl
import time
from google.cloud import bigquery
from loguru import logger
from tools.clusterisation_tools import (
    clusterisation_from_prebuild_embedding,
)


from tools.reduction_tools import reduce_embedding_by_chunk
from tools.utils import ENV_SHORT_NAME, export_polars_to_bq


def clusterization(
    target_n_clusters: int = typer.Option(
        ..., help="Split proportion between fit and predict"
    ),
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    clustering_group: str = typer.Option(
        "default-config",
        help="Config file name",
    ),
):
    logger.info("Loading data: fetch item with pretained encoding...")

    client = bigquery.Client()

    # TO DO: Add rights to fetch data straight to polars
    # bigquery_job = client.query(
    #     f"SELECT * from `tmp_{ENV_SHORT_NAME}.{input_table}` where category_group='{clustering_group}' "
    # )
    # rows = bigquery_job.result()
    # item_embedding_enriched = pl.from_arrow(rows.to_arrow())

    items_with_embedding_pd = pd.read_gbq(
        f"SELECT * from `tmp_{ENV_SHORT_NAME}.{input_table}` where category_group='{clustering_group}' "
    )
    logger.info(
        f"Loaded! -> item_full_encoding_enriched: {len(items_with_embedding_pd)}"
    )

    items_with_embedding = pl.from_pandas(items_with_embedding_pd)
    logger.info(
        f"Build cluster for category {clustering_group} with {len(items_with_embedding)} items..."
    )
    items_with_embedding = items_with_embedding.fill_nan(0)
    ##Reduction step
    item_embedding_components = items_with_embedding[["t0", "t1", "t2", "t3", "t4"]]
    item_embedding_2D = reduce_embedding_by_chunk(
        item_embedding_components, dim=2, chunk_size=int(2e4)
    )
    items_embedding_with_coordinates = pl.concat(
        [
            items_with_embedding,
            item_embedding_2D,
        ],
        how="horizontal",
    )
    ##Clusterisation step
    logger.info(f"Clusterisation step...")
    start = time.time()
    items_with_cluster_coordinates = clusterisation_from_prebuild_embedding(
        item_embedding_2D,
        int(target_n_clusters),
    )
    logger.info(f"Clusterisation done in: {int(int(time.time() - start)//60)} minutes.")
    items_fully_enriched = pl.concat(
        [
            items_embedding_with_coordinates,
            items_with_cluster_coordinates,
        ],
        how="horizontal",
    )
    logger.info(f"Clustering postprocessing... ")
    items_fully_enriched_clean = embedding_cleaning(items_fully_enriched)

    export_polars_to_bq(
        client, items_fully_enriched_clean, f"tmp_{ENV_SHORT_NAME}", output_table
    )
    return


def embedding_cleaning(items_fully_enriched):
    with pl.Config(auto_structify=True):
        items_fully_enriched = items_fully_enriched.with_columns(
            semantic_encoding=pl.col(["t0", "t1", "t2", "t3", "t4"])
        )
    items_fully_enriched = items_fully_enriched.drop(["t0", "t1", "t2", "t3", "t4"])
    items_fully_enriched = items_fully_enriched.select(
        "item_id", "cluster", "x", "y", "x_cluster", "y_cluster", "semantic_encoding"
    )

    return items_fully_enriched


if __name__ == "__main__":
    typer.run(clusterization)
