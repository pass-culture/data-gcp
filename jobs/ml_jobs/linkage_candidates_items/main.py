import typer
from utils import read_parquet, upload_parquet

import pandas as pd
import json
from lancedb.pydantic import Vector, LanceModel
from loguru import logger
from semantic_space_classes import TextClient
import subprocess
import typer

BIGQUERY_CLEAN_DATASET = "clean_prod"
LOCAL_RETRIEVAL_PATH = "./retrieval_vector"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"

import pandas as pd
import json
from lancedb.pydantic import Vector, LanceModel
from loguru import logger
from semantic_space_classes import TextClient
import subprocess
import typer
BIGQUERY_CLEAN_DATASET="clean_prod"
LOCAL_RETRIEVAL_PATH='./retrieval_vector'
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"

app = typer.Typer()


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    source_df = read_parquet(source_file_path)

    # Your code here
    output_df = source_df

    upload_parquet(
        dataframe=output_df,
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()




