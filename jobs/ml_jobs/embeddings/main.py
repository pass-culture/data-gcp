import concurrent
import json
import traceback
import typer
from itertools import repeat
from multiprocessing import cpu_count
import pandas as pd
from datetime import datetime
from tools.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    CONFIGS_PATH,
)
from tools.embedding_extraction import extract_embedding


def embedding_extraction(data, params, subset_length, batch_id):

    df_data_with_embedding = extract_embedding(
        data,
        params,
    )
    return df_data_with_embedding


def main(
    gcp_project: str = typer.Option(GCP_PROJECT_ID, help="GCP project ID"),
    env_short_name: str = typer.Option(ENV_SHORT_NAME, help="Env short name"),
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
) -> None:
    ###############
    # Load config
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)

    data_type = params["embedding_extract_from"]
    ###############
    # Load preprocessed data
    df_data_to_extract_embedding = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.tmp_{env_short_name}.{data_type}_to_extract_embeddings_clean`"
    )

    ###############
    # Run embedding extraction
    df_data_w_embedding = embedding_extraction(df_data_to_extract_embedding,params)

    df_data_w_embedding["extraction_date"] = [
        datetime.now().strftime("%Y-%m-%d")
    ] * len(df_data_w_embedding)
    df_data_w_embedding.to_gbq(
        f"clean_{env_short_name}.{data_type}_embeddings",
        project_id=gcp_project,
        if_exists="append",
    )


if __name__ == "__main__":
    typer.run(main)
