import json

import pandas as pd
import typer
from tools.config import CONFIGS_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID


def preprocess(df, feature_list):
    for feature in feature_list:
        df[feature] = df[feature].str.lower()
    return df


def main(
    gcp_project: str = typer.Option(
        GCP_PROJECT_ID,
        help="BigQuery Project in which the offers to link is located",
    ),
    env_short_name: str = typer.Option(
        ENV_SHORT_NAME,
        help="Environnement short name",
    ),
    config_file_name: str = typer.Option(
        "default-config-offer",
        help="Config file name",
    ),
) -> None:
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)
    data_type = params["embedding_extract_from"]
    df_data = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.tmp_{env_short_name}.{data_type}_to_extract_embeddings`"
    )

    df_data_clean = preprocess(
        df_data,
        [
            features["name"]
            for features in params["features"]
            if features["type"] == "text"
        ],
    )
    df_data_clean.to_gbq(
        f"tmp_{env_short_name}.{data_type}_to_extract_embeddings_clean",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
