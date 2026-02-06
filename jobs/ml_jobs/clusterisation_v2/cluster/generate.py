import time

import pandas as pd
import polars as pl
import typer
from loguru import logger

from tools.clusterisation import (
    clusterisation_from_prebuild_embedding,
)
from tools.utils import (
    MAX_EMBEDDING_SIZE,
    export_polars_to_bq,
    load_config_file,
    sha1_to_base64,
)


def create_clusters(
    input_dataset_name: str = typer.Option(..., help="Path to dataset input name"),
    input_table_name: str = typer.Option(..., help="Path to table intput name"),
    output_dataset_name: str = typer.Option(..., help="Path to the dataset name"),
    output_table_name: str = typer.Option(..., help="Path to tablename"),
    config_file_name: str = typer.Option(
        "default-config",
        help="Config file name",
    ),
):
    """
    Create clusters based on the given input data and configuration file {config_file_name}.

    Args:
        input_table (str): Path to the input data.
        output_table (str): Path to the output data.
        config_file_name (str, optional): Config file name. Defaults to "default-config".
    """

    params = load_config_file(config_file_name, job_type="cluster")

    # Validate embedding size
    raw_size = params.get("pretrained_embedding_size", 0)
    if not isinstance(raw_size, int):
        raise ValueError("pretrained_embedding_size must be an integer")

    if raw_size < 0 or raw_size > MAX_EMBEDDING_SIZE:
        raise ValueError(
            f"pretrained_embedding_size must be between 0 and {MAX_EMBEDDING_SIZE}"
        )
    embedding_size = raw_size

    embedding_cols = [f"t{x}" for x in range(embedding_size)]
    results = []
    for group in params["group_config"]:
        results.append(
            generate_clustering(
                group, input_dataset_name, input_table_name, embedding_cols
            )
        )

    logger.info(
        f"Exporting clusters to table: {output_dataset_name}.{output_table_name}"
    )
    export_polars_to_bq(pl.concat(results), output_dataset_name, output_table_name)
    logger.info(
        f"Clusters successfully exported to: {output_dataset_name}.{output_table_name}"
    )


def generate_clustering(
    group: dict, input_dataset_name: str, input_table_name: str, embedding_cols: list
) -> pl.DataFrame:
    target_n_clusters = group["nb_clusters"]
    clustering_group = group["group"]
    logger.info(f"Will load {input_dataset_name}.{input_table_name}...")
    items_with_embedding_pd = pd.read_gbq(
        f"SELECT * from `{input_dataset_name}.{input_table_name}` where category_group='{clustering_group}' "
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
    item_embedding_components = items_with_embedding[embedding_cols]

    ##Clusterisation step
    logger.info("Clusterisation step...")
    start = time.time()
    items_with_cluster_coordinates = clusterisation_from_prebuild_embedding(
        item_embedding_components,
        int(target_n_clusters),
    )
    logger.info(f"Clusterisation done in: {int(int(time.time() - start)//60)} minutes.")
    items_fully_enriched = pl.concat(
        [
            items_with_embedding[["item_id"]],
            item_embedding_components,
            items_with_cluster_coordinates,
        ],
        how="horizontal",
    )
    logger.info("Clustering postprocessing... ")
    return embedding_cleaning(items_fully_enriched, clustering_group, embedding_cols)


def embedding_cleaning(items_fully_enriched, clustering_group, embedding_cols):
    with pl.Config(auto_structify=True):
        items_fully_enriched = items_fully_enriched.with_columns(
            category=pl.lit(clustering_group),
            semantic_encoding=pl.col(embedding_cols),
            semantic_category=pl.concat_str(
                [pl.lit(clustering_group), pl.col("cluster")], separator="-"
            ),
        ).with_columns(
            semantic_cluster_id=pl.col("semantic_category").map_elements(sha1_to_base64)
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
