import json
from datetime import datetime

import pandas as pd
import typer

from tools.config import CONFIGS_PATH, GCP_PROJECT_ID
from tools.embedding_extraction import extract_embedding
from utils.logging import logging


def preprocess(df, features):
    df = df.fillna(" ")
    for feature in features:
        if feature["type"] == "macro_text":
            df[feature["name"]] = (
                df[feature["content"]]
                .agg(" ".join, axis=1)
                .astype(str)
                .str.lower()
                .str.replace("_", " ")
            )
    return df


def load_data(
    gcp_project: str,
    input_dataset_name: str,
    input_table_name: str,
    max_rows_to_process: int,
) -> pd.DataFrame:
    # If max_rows_to_process is -1, we will process all data.
    limit = f" LIMIT {max_rows_to_process} " if max_rows_to_process > 0 else ""
    return pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.{input_dataset_name}.{input_table_name}` {limit} "
    )


def main(
    gcp_project: str = typer.Option(GCP_PROJECT_ID, help="GCP project ID"),
    config_file_name: str = typer.Option(
        "default-config-offer", help="Config file name"
    ),
    batch_size: int = typer.Option(
        ...,
        help="Number of items to preprocess",
    ),
    max_rows_to_process: int = typer.Option(
        -1,
        help="Number of item to preprocess at maximum. If -1, all items will be processed.",
    ),
    input_dataset_name: str = typer.Option(
        ...,
        help="Name of the input dateset",
    ),
    input_table_name: str = typer.Option(
        ...,
        help="Name of the input table.",
    ),
    output_dataset_name: str = typer.Option(
        ...,
        help="Name of the output dataset",
    ),
    output_table_name: str = typer.Option(
        ...,
        help="Name of the output table",
    ),
) -> None:
    """
    Main loggic data for embedding extraction.
    Input table depends on output table, we loop until all data is processed, by default.
    """

    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)

    processed_rows = 0
    iteration = 0

    df = load_data(
        gcp_project,
        input_dataset_name,
        input_table_name,
        max_rows_to_process,
    )

    if df.shape[0] > 0:
        logging.info(f"Will process {df.shape} rows.")
        df = preprocess(df, params["features"])

        for start in range(0, df.shape[0], batch_size):
            df_subset = df.iloc[start : start + batch_size]
            extract_embedding(df_subset, params).assign(
                extraction_date=datetime.now().strftime("%Y-%m-%d"),
                extraction_datetime=datetime.now(),
            ).to_gbq(
                f"{output_dataset_name}.{output_table_name}",
                project_id=gcp_project,
                if_exists="append",
            )
            logging.info(
                f"{iteration} iteration (batch {start}/{start + batch_size}), processed rows: {processed_rows}"
            )

            processed_rows += df_subset.shape[0]
            iteration += 1
    else:
        logging.info("Nothing to update !")


if __name__ == "__main__":
    typer.run(main)
