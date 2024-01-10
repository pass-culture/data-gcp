import json
from datetime import datetime

import numpy as np
import pandas as pd
import typer
from loguru import logger
from tools.utils import (
    CONFIGS_PATH,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    convert_str_emb_to_float,
    convert_arr_emb_to_str,
)
from tools.dimension_reduction import (
    umap_reduce_embedding_dimension,
    export_polars_to_bq,
    pca_reduce_embedding_dimension,
    pumap_reduce_embedding_dimension,
)
import pyarrow.dataset as ds
import polars as pl


def export_reduction_table(
    df, target_dimension, embedding_columns, method="PUMAP", max_dimension=32
):

    for emb_col in embedding_columns:
        logger.info(f"Convert serialized embeddings... {emb_col}...")
        X = np.array(convert_str_emb_to_float(df[emb_col]))
        # reduce first with PCA
        if method in ("UMAP", "PUMAP"):
            logger.info(f"Reducing first with PCA {emb_col}...")
            X = pca_reduce_embedding_dimension(X, dimension=max_dimension)

        if method == "UMAP":
            logger.info(f"Reducing with UMAP {emb_col}...")
            X = umap_reduce_embedding_dimension(X, target_dimension)
        elif method == "PUMAP":
            logger.info(f"Reducing with PUMAP {emb_col}...")
            X = pumap_reduce_embedding_dimension(
                X, target_dimension, batch_size=2048, train_frac=0.1
            )
        elif method == "PCA":
            logger.info(f"Reducing with PCA {emb_col}...")
            X = pca_reduce_embedding_dimension(X, dimension=target_dimension)
        else:
            raise Exception("Metohd not found.")
        # transform as str again
        logger.info(f"Transform embeddings as str for {emb_col}...")
        X = convert_arr_emb_to_str(X)
        logger.info(f"Process done {emb_col}...")
        df = df.with_columns(pl.Series(name=emb_col, values=X))
        logger.info(f"Done for {emb_col}...")

    return df.with_columns(reduction_method=pl.lit(method))


def plan(
    source_gs_path,
    embedding_columns,
    output_table_prefix,
    target_dimension,
    gcp_project,
    env_short_name,
    method,
    max_dimension,
):
    dataset = ds.dataset(source_gs_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    export_cols = ["item_id"] + embedding_columns
    output_table_name = f"{output_table_prefix}_reduced_{target_dimension}"
    logger.info(f"Reducing Table... {output_table_name}")
    ldf = export_reduction_table(
        ldf.select(export_cols).collect(),
        target_dimension=target_dimension,
        embedding_columns=embedding_columns,
        method=method,
        max_dimension=max_dimension,
    )
    export_polars_to_bq(
        ldf.select(["item_id", "reduction_method"] + embedding_columns),
        project_id=gcp_project,
        dataset=f"clean_{env_short_name}",
        output_table=output_table_name,
    )
    logger.info(f"Done Table... {output_table_name}")


def dimension_reduction(
    gcp_project: str = typer.Option(GCP_PROJECT_ID, help="GCP project ID"),
    env_short_name: str = typer.Option(ENV_SHORT_NAME, help="Env short name"),
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
    source_gs_path: str = typer.Option(
        ...,
        help="Name of the dataframe we want to reduce",
    ),
    output_table_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    reduction_config: str = typer.Option(
        "default",
        help="String for the configuration plan to execute",
    ),
) -> None:
    """ """
    ###############
    # Load config
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        config_json = json.load(config_file)["reduction_configs"][reduction_config]

    ###############
    # Load preprocessed data
    for params in config_json:
        target_dimension = params["target_dimensions"]
        method = params["method"]
        embedding_columns = params["embedding_columns"]
        pre_reduction_dim = params.get("pca_pre_reduction_dimension", 32)
        plan(
            source_gs_path,
            embedding_columns,
            output_table_prefix=output_table_name,
            target_dimension=target_dimension,
            gcp_project=gcp_project,
            env_short_name=env_short_name,
            method=method,
            max_dimension=pre_reduction_dim,
        )


if __name__ == "__main__":
    typer.run(dimension_reduction)
