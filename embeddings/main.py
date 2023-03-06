import concurrent
import traceback
from itertools import repeat
from multiprocessing import cpu_count
import pandas as pd

from tools.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    CONFIGS_PATH,
)
from tools.embedding_extraction import extract_embedding


def embedding_extraction(data, params, batch_number, batch_id):

    try:
        df_data_to_extract_embedding_current_batch = data[
            batch_id * batch_number : (batch_id + 1) * batch_number
        ]
        df_data_with_embedding = extract_embedding(
            df_data_to_extract_embedding_current_batch,
            params,
        )
        df_data_with_embedding.to_gbq()
        return True
    except Exception as e:
        print(e)
        traceback.print_exc()
        return False


def main(
    gcp_project,
    env_short_name,
    config_file_name,
) -> None:
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
    df_data_to_extract_embedding = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.sandbox_{env_short_name}.offers_to_link_clean`"
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
            repeat(config_file_name),
            repeat(batch_number),
            range(batch_number),
        )
        for future in futures:
            df_data_with_embedding_df_list.append(future)
    print("Multiprocessing done")
    df_data_w_embedding = pd.concat(df_data_with_embedding_df_list)
    df_data_w_embedding.to_gbq(
        f"sandbox_{env_short_name}.linked_offers_full",
        project_id=gcp_project,
        if_exists="replace",
    )
    # Save already extracted data

    # Cast offer_id back to string
    df_data_to_extract_embedding["offer_id"] = df_data_to_extract_embedding[
        "offer_id"
    ].astype(str)
    df_data_to_extract_embedding.to_gbq(
        f"analytics_{env_short_name}.offers_already_embedded",
        project_id=gcp_project,
        if_exists="append",
    )


if __name__ == "__main__":
    main(GCP_PROJECT_ID, ENV_SHORT_NAME)
