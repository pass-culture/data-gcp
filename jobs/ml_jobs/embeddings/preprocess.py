import json

import pandas as pd
import typer
from tools.config import CONFIGS_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID


def preprocess(df, features):
    df = df.fillna(" ")
    for feature in features:
        if feature["type"] == "text":
            df[feature["name"]] = df[feature["name"]].str.lower()
        if feature["type"] == "macro_text":
            df[feature["name"]] = df[feature["content"]].agg(" ".join, axis=1)
            df[feature["name"]] = df[feature["name"]].astype(str)
            df[feature["name"]] = df[feature["name"]].str.lower()
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
    input_table_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    output_table_name: str = typer.Option(
        ...,
        help="Name of the cleaned dataframe",
    ),
) -> None:
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)
    df_data = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.tmp_{env_short_name}.{input_table_name}`"
    )

    df_data_clean = preprocess(df_data, params["features"])
    df_data_clean.to_gbq(
        f"tmp_{env_short_name}.{output_table_name}",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
