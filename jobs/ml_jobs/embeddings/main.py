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
    Main loggic data for embedding extraction
    """

    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)

    batch_size = params["batch_size"]

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

    # input table depends on output table.
    while df.shape[0] > 0:
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
