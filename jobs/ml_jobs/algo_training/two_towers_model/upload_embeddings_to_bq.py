import datetime

import mlflow.tensorflow
import numpy as np
import pandas as pd
import tensorflow as tf
import typer
from google.cloud import bigquery
from loguru import logger

from commons.constants import (
    GCP_PROJECT_ID,
    MLFLOW_RUN_ID_FILENAME,
    MODEL_DIR,
)
from commons.mlflow_tools import connect_remote_mlflow


def main(
    experiment_name: str = typer.Option(None, help="Name of the experiment on MLflow"),
    run_name: str = typer.Option(
        None, help="GCP BQ dataset to put users and items embeddings tables in"
    ),
    dataset_id: str = typer.Option(
        "raw-prod", help="GCP BQ dataset to put users and items embeddings tables in"
    ),
):
    """
    Retrieves information and model from mlflow and uploads user and items embeddings to table in dataset_id.
    """
    connect_remote_mlflow()
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    with open(f"{MODEL_DIR}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="r") as file:
        run_id = file.read()
    with mlflow.start_run(experiment_id=experiment_id, run_id=run_id) as run:
        artifact_uri = mlflow.get_artifact_uri("model")
        loaded_model = tf.keras.models.load_model(
            artifact_uri,
            compile=False,
        )

    logger.info("------ RECO EMBEDDINGS LOGGING STARTED ------------")

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)

    ## User embeddings
    user_list = loaded_model.user_layer.layers[0].get_vocabulary()
    user_weights = loaded_model.user_layer.layers[1].get_weights()[0].astype(np.float64)
    user_table_id = f"{GCP_PROJECT_ID}.{dataset_id}.two_tower_user_embedding"
    user_embedding_df = pd.DataFrame(
        {
            "train_date": [
                datetime.datetime.fromtimestamp(
                    run.info.start_time / 1000, tz=datetime.timezone.utc
                )
            ]
            * len(user_list),
            "mlflow_run_id": [run_id] * len(user_list),
            "mlflow_experiment_name": [str(experiment_name)] * len(user_list),
            "mlflow_run_name": [run_name] * len(user_list),
            "user_id": user_list,
            "user_embedding": list(user_weights),
        }
    )

    user_embed_job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("train_date", "DATE"),
            bigquery.SchemaField("mlflow_run_id", "STRING"),
            bigquery.SchemaField("mlflow_experiment_name", "STRING"),
            bigquery.SchemaField("mlflow_run_name", "STRING"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("user_embedding", "FLOAT64", mode="repeated"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="train_date",
            expiration_ms=1000 * 60 * 60 * 24 * 30 * 6,  # expires in 6M
        ),
    )
    load_job = bq_client.load_table_from_dataframe(
        user_embedding_df, user_table_id, job_config=user_embed_job_config
    )
    result = load_job.result()

    logger.info("Written {} rows to {}".format(result.output_rows, result.destination))
    logger.info("Partitioning: {}".format(result.time_partitioning))

    ## Items embeddings
    item_list = loaded_model.item_layer.layers[0].get_vocabulary()
    item_weights = loaded_model.item_layer.layers[1].get_weights()[0].astype(np.float64)
    item_table_id = f"{GCP_PROJECT_ID}.{dataset_id}.two_tower_item_embedding"

    item_embedding_df = pd.DataFrame(
        {
            "train_date": [
                datetime.datetime.fromtimestamp(
                    run.info.start_time / 1000, tz=datetime.timezone.utc
                )
            ]
            * len(item_list),
            "mlflow_run_id": [run_id] * len(item_list),
            "mlflow_experiment_name": [str(experiment_name)] * len(item_list),
            "mlflow_run_name": [run_name] * len(item_list),
            "item_id": item_list,
            "item_embedding": list(item_weights),
        }
    )

    item_embed_job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("train_date", "DATE"),
            bigquery.SchemaField("mlflow_run_id", "STRING"),
            bigquery.SchemaField("mlflow_experiment_name", "STRING"),
            bigquery.SchemaField("mlflow_run_name", "STRING"),
            bigquery.SchemaField("item_id", "STRING"),
            bigquery.SchemaField("item_embedding", "FLOAT64", mode="repeated"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="train_date",
            expiration_ms=1000 * 60 * 60 * 24 * 30 * 6,  # expires in 6M
        ),
    )

    load_job = bq_client.load_table_from_dataframe(
        item_embedding_df, item_table_id, job_config=item_embed_job_config
    )

    result = load_job.result()

    logger.info("Written {} rows to {}".format(result.output_rows, result.destination))
    logger.info("Partitioning: {}".format(result.time_partitioning))

    logger.info("------ RECO EMBEDDINGS LOGGING ENDED ------------")


if __name__ == "__main__":
    typer.run(main)
