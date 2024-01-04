import json
from datetime import datetime

import numpy as np
import pandas as pd
import typer
from loguru import logger
from tools.config import CONFIGS_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID
from tools.dimension_reduction import reduce_embedding_dimension
import pyarrow.dataset as ds
import polars as pl


def export_reduction_table(df, dimension, embedding_columns):
    for emb_col in embedding_columns:
        logger.info(f"Reducing {emb_col}...")
        df[emb_col] = reduce_embedding_dimension(df[emb_col], dimension).tolist()
        df[emb_col] = df[emb_col].astype(str)
        logger.info(f"Done for {emb_col}...")
    return df


def dimension_reduction(
    gcp_project: str = typer.Option(GCP_PROJECT_ID, help="GCP project ID"),
    env_short_name: str = typer.Option(ENV_SHORT_NAME, help="Env short name"),
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
    source_gs_path: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    output_table_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
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
        params = json.load(config_file)

    ###############
    # Load preprocessed data

    dataset = ds.dataset(source_gs_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    df_data_w_embedding = ldf.collect().to_pandas()

    for reduction_dim_str, embedding_columns in params["reduction_plan"].items():
        logger.info(f"Reducing Table... {output_table_name}_{reduction_dim_str}")
        export_df = export_reduction_table(
            df_data_w_embedding.copy(),
            dimension=int(reduction_dim_str),
            embedding_columns=embedding_columns,
        )
        export_df.to_gbq(
            f"clean_{env_short_name}.{output_table_name}_{reduction_dim_str}",
            project_id=gcp_project,
            if_exists="replace",
        )
        logger.info(f"Done Table... {output_table_name}_{reduction_dim_str}")

if __name__ == "__main__":
    typer.run(dimension_reduction)
