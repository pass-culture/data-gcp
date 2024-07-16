import json
from datetime import datetime
import pandas as pd
import typer
from utils.logging import logging
from tools.config import CONFIGS_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID
from tools.embedding_extraction import extract_embedding


def preprocess(df, features):
    df = df.fillna(" ")
    for feature in features:
        if feature["type"] == "text":
            df[feature["name"]] = df[feature["name"]].str.lower()
        if feature["type"] == "macro_text":
            df[feature["name"]] = df[feature["content"]].agg(" ".join, axis=1)
            df[feature["name"]] = df[feature["name"]].astype(str)
            df[feature["name"]] = df[feature["name"]].str.lower()
            df[feature["name"]] = df[feature["name"]].str.replace("_", " ")
    return df


def load_data(
    gcp_project: str,
    input_dataset_name: str,
    input_table_name: str,
    batch_size: int,
    iteration: int,
    processed_rows: int,
) -> pd.DataFrame:
    sql = f"SELECT * FROM `{gcp_project}.{input_dataset_name}.{input_table_name}` LIMIT {batch_size} "

    df = pd.read_gbq(sql)
    logging.info(
        f"Data loaded: {df.shape}, {iteration} iteration, processed rows: {processed_rows}"
    )
    return df


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
        help="Number of item to preprocess at maximum. If -1, all items will be processed",
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

    batch_size = params["batch_size"]

    count_query = f"SELECT COUNT(*) as total_to_process FROM `{gcp_project}.{input_dataset_name}.{input_table_name}`"
    total_to_process = pd.read_gbq(count_query)["total_to_process"][0]

    if max_rows_to_process < 0:
        max_rows_to_process = total_to_process

    logging.info(
        f"Total rows to process: {total_to_process}, will process {max_rows_to_process} rows."
    )

    df = load_data(
        gcp_project,
        input_dataset_name,
        input_table_name,
        batch_size,
        iteration,
        processed_rows,
    )
    processed_rows = 0
    iteration = 0

    # Will loop until all data is processed or max_rows_to_process is reached
    while df.shape[0] > 0 and processed_rows < max_rows_to_process:
        df = preprocess(df, params["features"])

        df = (
            extract_embedding(
                df,
                params,
            )
            .assign(
                extraction_date=datetime.now().strftime("%Y-%m-%d"),
                extraction_datetime=datetime.now(),
            )
            .to_gbq(
                f"{output_dataset_name}.{output_table_name}",
                project_id=gcp_project,
                if_exists="append",
            )
        )
        processed_rows += df.shape[0]
        iteration += 1
        # loop until all data is processed
        df = load_data(
            gcp_project,
            input_dataset_name,
            input_table_name,
            batch_size,
            iteration,
            processed_rows,
        )

    return


if __name__ == "__main__":
    typer.run(main)
