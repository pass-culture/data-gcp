import os
import subprocess

import pandas as pd
from google.cloud import bigquery

# from loguru import logger

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"


def read_sql_query(file_path: str) -> str:
    """Read SQL query from a file and return as a string."""
    with open(file_path) as file:
        return file.read()


def get_user_data_from_query() -> pd.DataFrame:
    """Fetch user data from BigQuery using the SQL query and return as pandas DataFrame."""
    query = read_sql_query("sql/user_data.sql")
    client = bigquery.Client(project="passculture-data-prod")
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df


def get_model_from_mlflow(
    experiment_name: str, run_id: str = None, artifact_uri: str = None
):
    client = bigquery.Client()

    # get artifact_uri from BQ
    if artifact_uri is None or len(artifact_uri) <= 10:
        if run_id is None or len(run_id) <= 2:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        else:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        if len(results_array) == 0:
            raise Exception(
                f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}. Failing."
            )
        else:
            artifact_uri = results_array[0]["artifact_uri"]
    return artifact_uri


def download_model(artifact_uri: str) -> None:
    """
    Download model from GCS bucket
    Args:
        artifact_uri (str): GCS bucket path
    """
    command = f"gsutil -m cp -r {artifact_uri} ."
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    # for line in results.stdout:
    # logger.info(line.rstrip().decode("utf-8"))
