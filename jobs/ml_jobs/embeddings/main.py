import json
from datetime import datetime
import pandas as pd
import typer

from tools.config import CONFIGS_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID
from tools.embedding_extraction import extract_embedding


def main(
    gcp_project: str = typer.Option(GCP_PROJECT_ID, help="GCP project ID"),
    env_short_name: str = typer.Option(ENV_SHORT_NAME, help="Env short name"),
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
    input_table_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    output_table_name: str = typer.Option(
        ...,
        help="Name of the cleaned dataframe",
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
    df_data_to_extract_embedding = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.tmp_{env_short_name}.{input_table_name}`"
    )

    ###############
    # Run embedding extraction
    df_data_w_embedding, emb_size_dict = extract_embedding(
        df_data_to_extract_embedding,
        params,
    )

    df_data_w_embedding["extraction_date"] = [
        datetime.now().strftime("%Y-%m-%d")
    ] * len(df_data_w_embedding)

    df_data_w_embedding.to_gbq(
        f"clean_{env_short_name}.{output_table_name}",
        project_id=gcp_project,
        if_exists="append",
    )

    with open("./emb_size_dict.json", "w") as f:
        json.dump(emb_size_dict, f)
    return


if __name__ == "__main__":
    typer.run(main)
