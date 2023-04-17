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

    try:
        df_data_to_extract_embedding_current_batch = data[
            batch_id * subset_length : (batch_id + 1) * subset_length
        ]
        df_data_with_embedding = extract_embedding(
            df_data_to_extract_embedding_current_batch,
            params,
        )
        return df_data_with_embedding
    except Exception as e:
        print(e)
        traceback.print_exc()
        return False


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
    max_process = cpu_count() - 1

    df_data_with_embedding_df_list = []
    subset_length = len(df_data_to_extract_embedding) // max_process
    subset_length = subset_length if subset_length > 0 else 1
    batch_number = max_process if subset_length > 1 else 1
    print(
        f"Starting process... with {batch_number} CPUs, subset length: {subset_length} "
    )
    with concurrent.futures.ProcessPoolExecutor(max_process) as executor:
        futures = executor.map(
            embedding_extraction,
            repeat(df_data_to_extract_embedding),
            repeat(params),
            repeat(subset_length),
            range(batch_number),
        )
        for future in futures:
            df_data_with_embedding_df_list.append(future)
    print("Multiprocessing done")
    df_data_w_embedding = pd.concat(df_data_with_embedding_df_list)

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
