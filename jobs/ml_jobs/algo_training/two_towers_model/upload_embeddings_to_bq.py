import os

################################  To use Keras 2 instead of 3  ################################
# See [TensorFlow + Keras 2 backwards compatibility section](https://keras.io/getting_started/)
os.environ["TF_USE_LEGACY_KERAS"] = "1"
###############################################################################################
import datetime

import mlflow
import numpy as np
import pandas as pd
import tensorflow as tf
import typer
from google.cloud import bigquery
from loguru import logger

from commons.constants import (
    EMBEDDING_EXPIRATION_DELAY_MS,
    GCP_PROJECT_ID,
    MLFLOW_RUN_ID_FILENAME,
    MODEL_DIR,
)
from commons.mlflow_tools import connect_remote_mlflow


def upload_embeddings_to_bigquery(
    field_str: str,
    dataset_id: str,
    train_date: datetime.datetime,
    run_id: str,
    experiment_name: str,
    run_name: str,
    ids_list: list[str],
    embeddings_list: list[list[float]],
) -> None:
    """
    Uploads embeddings to a BigQuery table with time partitioning.

    Parameters:
        field_str (str): The field name used to identify the embeddings  "user" or "item".
        dataset_id (str): The BigQuery dataset ID where the table resides.
        train_date (datetime.date): The training date to associate with the embeddings.
        run_id (str): The MLflow run ID associated with the training from which we got the embeddings.
        experiment_name (str): The name of the MLflow experiment.
        run_name (str): The name of the MLflow run.
        ids_list (list[str]): List of unique IDs corresponding to the embeddings of the item or the user.
        embeddings_list (list[list[float]]): List of embeddings, where each embedding is a list of float values.

    Returns:
        None

    Raises:
        google.cloud.exceptions.GoogleCloudError: If the data upload to BigQuery fails.

    Logs:
        - Logs the start and completion of the embedding upload.
        - Logs the number of rows written and partitioning information.

    Notes:
        - The function creates a DataFrame with the embeddings and additional metadata.
        - The BigQuery table is named dynamically based on the `field_str` parameter.
        - The table is time-partitioned by the `train_date` field.
    """

    logger.info(f"Started uploading {field_str} embeddings")

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    table_id = f"{GCP_PROJECT_ID}.{dataset_id}.two_tower_{field_str}_embedding_history"

    embedding_df = pd.DataFrame(
        {
            f"{field_str}_id": ids_list,
            f"{field_str}_embedding": embeddings_list,
        }
    ).assign(
        train_date=train_date,
        mlflow_run_id=run_id,
        mlflow_experiment_name=experiment_name,
        mlflow_run_name=run_name,
    )

    embed_job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("train_date", "DATE"),
            bigquery.SchemaField("mlflow_run_id", "STRING"),
            bigquery.SchemaField("mlflow_experiment_name", "STRING"),
            bigquery.SchemaField("mlflow_run_name", "STRING"),
            bigquery.SchemaField(f"{field_str}_id", "STRING"),
            bigquery.SchemaField(f"{field_str}_embedding", "FLOAT64", mode="repeated"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="train_date",
            expiration_ms=EMBEDDING_EXPIRATION_DELAY_MS,
        ),
    )
    load_job = bq_client.load_table_from_dataframe(
        embedding_df, table_id, job_config=embed_job_config
    )
    result = load_job.result()

    logger.info(f"Written {result.output_rows} rows to {result.destination}")
    logger.info(f"Partitioning: {result.time_partitioning}")


def main(
    experiment_name: str = typer.Option(None, help="Name of the experiment on MLflow"),
    run_name: str = typer.Option(
        None, help="GCP BQ dataset to put users and items embeddings tables in"
    ),
    dataset_id: str = typer.Option(
        None, help="GCP BQ dataset to put users and items embeddings tables in"
    ),
):
    """
    Main function to upload user and item embeddings to BigQuery provided by a trained MLflow model .

    This function connects to a remote MLflow server, retrieves the run ID, loads a trained TensorFlow model,
    extracts user and item embeddings, and uploads them to a partitioned BigQuery table.

    Parameters:
        experiment_name (str): Name of the MLflow experiment to retrieve the experiment ID.
        dataset_id (str): The BigQuery dataset ID where embeddings will be uploaded.
        run_name (str): The name of the current MLflow run.

    Returns:
        None

    Raises:
        FileNotFoundError: If the run ID file is not found in the specified directory.
        google.cloud.exceptions.GoogleCloudError: If uploading embeddings to BigQuery fails.

    Workflow:
        1. Connect to the remote MLflow server.
        2. Retrieve the experiment ID for the specified experiment name.
        3. Read the run ID from a file.
        4. Start an MLflow run using the retrieved experiment ID and run ID.
        5. Load the TensorFlow model from the MLflow model URI.
        6. Extract user and item embeddings from the model.
        7. Upload user embeddings to BigQuery using `upload_embeddings_to_bigquery`.
        8. Upload item embeddings to BigQuery using `upload_embeddings_to_bigquery`.

    Logs:
        - Logs the start and end of the user and item embedding upload process.

    Notes:
        - The BigQuery tables are time-partitioned by the `train_date`.
        - Ensure the model directory and file structure are correctly set up before running.
    """
    connect_remote_mlflow()
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    with open(f"{MODEL_DIR}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="r") as file:
        run_id = file.read()
    with mlflow.start_run(experiment_id=experiment_id, run_id=run_id) as run:
        model_uri = mlflow.get_artifact_uri("model")
        loaded_model = tf.keras.models.load_model(
            model_uri,
            compile=False,
        )
    train_date = datetime.datetime.fromtimestamp(
        run.info.start_time / 1000, tz=datetime.timezone.utc
    )

    logger.info(
        "------ USERS AND ITEMS EMBEDDINGS UPLOAD TO BIGQUERY TASK STARTED ------------"
    )

    ## User embeddings
    user_ids_list = loaded_model.user_layer.layers[0].get_vocabulary()
    user_embedding_list = list(
        loaded_model.user_layer.layers[1].get_weights()[0].astype(np.float64)
    )
    upload_embeddings_to_bigquery(
        field_str="user",
        dataset_id=dataset_id,
        train_date=train_date,
        run_id=run_id,
        experiment_name=experiment_name,
        run_name=run_name,
        ids_list=user_ids_list,
        embeddings_list=user_embedding_list,
    )

    ## Items embeddings
    item_ids_list = loaded_model.item_layer.layers[0].get_vocabulary()
    item_embedding_list = list(
        loaded_model.item_layer.layers[1].get_weights()[0].astype(np.float64)
    )
    upload_embeddings_to_bigquery(
        field_str="item",
        dataset_id=dataset_id,
        train_date=train_date,
        run_id=run_id,
        experiment_name=experiment_name,
        run_name=run_name,
        ids_list=item_ids_list,
        embeddings_list=item_embedding_list,
    )

    logger.info(
        "------ USERS AND ITEMS EMBEDDINGS UPLOAD TO BIGQUERY TASK ENDED ------------"
    )


if __name__ == "__main__":
    typer.run(main)
