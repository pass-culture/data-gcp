import json
from datetime import datetime

import numpy as np
import pandas as pd
import typer
import umap
from loguru import logger
from tools.config import CONFIGS_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID
from tools.embedding_extraction import extract_embedding
from tools.dimension_reduction import reduce_embedding_dimension


def dimension_reduction(
    gcp_project: str = typer.Option(GCP_PROJECT_ID, help="GCP project ID"),
    env_short_name: str = typer.Option(ENV_SHORT_NAME, help="Env short name"),
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
    input_table_name: str = typer.Option(
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

    emb_size_dict = {}
    with open(
        "./emb_size_dict.json",
        mode="r",
        encoding="utf-8",
    ) as emb_size_file:
        emb_size_dict = json.load(emb_size_file)
    logger.info(f"emb_size_dict: {emb_size_dict}")
    ###############
    # Load preprocessed data
    df_data_w_embedding = pd.read_gbq(
        f"SELECT * FROM `tmp_{env_short_name}.{input_table_name}_v2`"
    )
    reduced_emb_df_init = df_data_w_embedding[["item_id", "extraction_date"]].astype(
        str
    )
    for dim in params["reduction_dimensions"]:
        emb_cols = [
            col
            for col in df_data_w_embedding.columns.tolist()
            if col not in ["item_id", "extraction_date"]
        ]
        reduced_emb_dict = {}
        for emb_col in emb_cols:
            logger.info(f"Reducing {emb_col}...")
            reduced_emb_dict[emb_col] = reduce_embedding_dimension(
                data=df_data_w_embedding[emb_col].tolist(),
                emb_size=emb_size_dict[emb_col],
                dimension=dim,
            )
        reduce_emb_df = pd.DataFrame(reduced_emb_dict)
        reduce_emb_df = reduce_emb_df.astype(str)
        final_reduced_emb = pd.concat([reduced_emb_df_init, reduce_emb_df], axis=1)
        final_reduced_emb.to_gbq(
            f"clean_{env_short_name}.{input_table_name}_reduced_{dim}",
            project_id=gcp_project,
            if_exists="append",
        )

    return


if __name__ == "__main__":
    typer.run(dimension_reduction)
