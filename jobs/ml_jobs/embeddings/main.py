import json
from datetime import datetime, timezone

import pandas as pd
import typer

from tools.config import (
    BIGQUERY_TMP_DATASET,
    CONFIGS_PATH,
    GCP_PROJECT_ID,
    NULLIFY_DEPRECATED_COLUMNS,
    PRIMARY_KEY,
)
from tools.embedding_extraction import extract_embedding
from utils.bigquery import (
    load_data,
    merge_upsert,
    write_to_tmp,
)
from utils.logging import logging


def preprocess(df: pd.DataFrame, features: list[dict]) -> pd.DataFrame:
    columns = list(
        set(
            [
                content
                for x in features
                if x["type"] == "macro_text"
                for content in x["content"]
            ]
        )
    )
    columns = list(set(columns))
    df[columns] = df[columns].fillna(" ")

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
    ts: str = typer.Option(
        default=datetime.now(timezone.utc).isoformat(),
        help="Processing timestamp",
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

    extraction_datetime = datetime.fromisoformat(ts)
    extraction_date = extraction_datetime.strftime("%Y-%m-%d")
    tmp_table = f"{BIGQUERY_TMP_DATASET}.tmp_embedding_extraction"
    main_table = f"{output_dataset_name}.{output_table_name}"

    df = load_data(
        gcp_project,
        input_dataset_name,
        input_table_name,
        max_rows_to_process,
    )

    if df.shape[0] > 0:
        logging.info(f"Will process {df.shape} rows.")
        df = preprocess(df, params["features"])

        for iteration, start in enumerate(range(0, df.shape[0], batch_size)):
            df_subset = df.iloc[start : start + batch_size]
            embeddings_df = extract_embedding(df_subset, params).assign(
                extraction_date=extraction_date,
                extraction_datetime=extraction_datetime,
            )

            if_exists = "replace" if iteration == 0 else "append"
            # Append to tmp table
            write_to_tmp(
                embeddings_df, tmp_table, if_exists=if_exists, project_id=gcp_project
            )

            processed_rows += df_subset.shape[0]
            logging.info(
                f"{iteration} iteration (batch {start}/{start + batch_size}), processed rows: {processed_rows}"
            )
            iteration += 1

        merge_upsert(
            tmp_table=tmp_table,
            main_table=main_table,
            primary_key=PRIMARY_KEY,
            project_id=gcp_project,
            date_columns=["extraction_date"],
            nullify_deprecated_columns=NULLIFY_DEPRECATED_COLUMNS,
        )
        logging.info("All batches processed and upserted successfully.")
    else:
        logging.info("Nothing to update !")


if __name__ == "__main__":
    typer.run(main)
