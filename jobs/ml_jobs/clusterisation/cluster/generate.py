import typer
import pandas as pd
import polars as pl
import time
from google.cloud import bigquery
from loguru import logger
from tools.clusterisation_tools import (
    clusterisation_from_prebuild_embedding,
)
from tools.utils import (
    ENV_SHORT_NAME,
    TMP_DATASET,
    CLEAN_DATASET,
    export_polars_to_bq,
    load_config_file,
    sha1_to_base64,
)


def create_clusters(
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config-offer",
        help="Config file name",
    ),
):
    client = bigquery.BigQuery()
    params = load_config_file(config_file_name)
    results = []
    for group in params["group_config"]:
        results.append(generate_clustering(group, input_table))

    export_polars_to_bq(client, pd.concat(results), CLEAN_DATASET, output_table)


def generate_clustering(group, input_table):
    target_n_clusters = group["nb_clusters"]
    clustering_group = group["group"]
    items_with_embedding_pd = pd.read_gbq(
        f"SELECT * from `{TMP_DATASET}.{input_table}` where category_group='{clustering_group}' "
    )
    logger.info(
        f"Loaded! -> item_full_encoding_enriched: {len(items_with_embedding_pd)}"
    )

    items_with_embedding = pl.from_pandas(items_with_embedding_pd)
    logger.info(
        f"Build cluster for category {clustering_group} with {len(items_with_embedding)} items..."
    )
    items_with_embedding = items_with_embedding.fill_nan(0)
    ## Reduction step
    item_embedding_components = items_with_embedding[["t0", "t1", "t2", "t3", "t4"]]

    ##Clusterisation step
    logger.info(f"Clusterisation step...")
    start = time.time()
    items_with_cluster_coordinates = clusterisation_from_prebuild_embedding(
        item_embedding_components,
        int(target_n_clusters),
    )
    logger.info(f"Clusterisation done in: {int(int(time.time() - start)//60)} minutes.")
    items_fully_enriched = pl.concat(
        [
            item_embedding_components,
            items_with_cluster_coordinates,
        ],
        how="horizontal",
    )
    logger.info(f"Clustering postprocessing... ")
    return embedding_cleaning(items_fully_enriched)


def embedding_cleaning(items_fully_enriched, clustering_group):
    with pl.Config(auto_structify=True):
        items_fully_enriched = items_fully_enriched.with_columns(
            category=clustering_group,
            semantic_encoding=pl.col(["t0", "t1", "t2", "t3", "t4"]),
            semantic_category=pl.concat_str(
                [clustering_group, pl.col("cluster")], separator="-"
            ),
        ).with_columns(
            semantic_cluster_id=pl.col("semantic_category").map(sha1_to_base64)
        )

    return items_fully_enriched.select(
        "item_id",
        "category",
        "semantic_category",
        "semantic_cluster_id",
        "x_cluster",
        "y_cluster",
        "semantic_encoding",
    )


if __name__ == "__main__":
    typer.run(create_clusters)
