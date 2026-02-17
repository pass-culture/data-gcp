import os

################################  To use Keras 2 instead of 3  ########################
# See [Keras 2 backwards compatibility section](https://keras.io/getting_started/)
os.environ["TF_USE_LEGACY_KERAS"] = "1"
#######################################################################################

import subprocess

import numpy as np
import pandas as pd
import tensorflow as tf
from google.cloud import bigquery
from loguru import logger

from constants import (
    BIGQUERY_CLEAN_DATASET,
    MODEL_BASE_PATH,
    MODELS_RESULTS_TABLE_NAME,
)


def fetch_user_item_data_with_embeddings(config):
    user_data_df = pd.read_parquet(f"{config['storage_path']}/user_data.parquet")
    item_data_df = pd.read_parquet(f"{config['storage_path']}/item_data.parquet")
    # Keep only necessary columns
    user_id_list = user_data_df["user_id"].unique().tolist()
    item_id_list = item_data_df["item_id"].unique().tolist()
    # Build mock ids list
    mock_user_id_list = [f"user_{i}" for i in range(config["number_of_mock_ids"])]
    mock_item_id_list = [f"item_{i}" for i in range(config["number_of_mock_ids"])]

    # Load TT
    # Load or download model from MLflow
    # Check if model already exists in MODEL_BASE_PATH
    loaded_model = download_and_load_model(config)
    logger.info("Two Tower model loaded.")

    logger.info("\nExtracting user and item embeddings...")
    # Get user and item embeddings from TT
    user_embedding_dict, item_embedding_dict = extract_embeddings(loaded_model)

    # Filter user IDs to those present in the model's user embedding vocabulary
    logger.info("Filtering IDs based on model's embedding vocabulary...")
    user_set = set(user_embedding_dict.keys())
    user_id_list = [uid for uid in user_id_list if uid in user_set]
    user_id_list = user_id_list[: config["number_of_ids"]]
    # Filter item IDs to those present in the model's item embedding vocabulary
    item_set = set(item_embedding_dict.keys())
    item_id_list = [iid for iid in item_id_list if iid in item_set]
    item_id_list = item_id_list[: config["number_of_ids"]]

    return (
        user_id_list,
        item_id_list,
        mock_user_id_list,
        mock_item_id_list,
        user_embedding_dict,
        item_embedding_dict,
    )


def extract_embeddings(loaded_model):
    item_list = loaded_model.item_layer.layers[0].get_vocabulary()
    item_weights = loaded_model.item_layer.layers[1].get_weights()[0].astype(np.float32)
    user_list = loaded_model.user_layer.layers[0].get_vocabulary()
    user_weights = loaded_model.user_layer.layers[1].get_weights()[0].astype(np.float32)
    user_embedding_dict = dict(zip(user_list, user_weights, strict=True))
    item_embedding_dict = dict(zip(item_list, item_weights, strict=True))
    return user_embedding_dict, item_embedding_dict


def download_and_load_model(config):
    if not os.path.exists(MODEL_BASE_PATH) or not os.listdir(MODEL_BASE_PATH):
        source_artifact_uri = get_model_from_mlflow(
            experiment_name=config["source_experiment_name"],
            run_id="",
            artifact_uri="",
        )
        logger.info(f"Model artifact_uri: {source_artifact_uri}")
        download_model(artifact_uri=source_artifact_uri)
    else:
        logger.info(f"Model already present in {MODEL_BASE_PATH}, skipping download.")
    logger.info("Model downloaded.")
    logger.info("Model directory contents:")
    model_files = os.listdir(MODEL_BASE_PATH)
    logger.info("\n".join(model_files))

    logger.info("\nLoad Two Tower model...")
    saved_model_path = os.path.join(os.getcwd(), MODEL_BASE_PATH)
    logger.info(f"Loading model from: {saved_model_path}")
    loaded_model = tf.keras.models.load_model(saved_model_path)
    logger.info(f"Model loaded successfully from: {saved_model_path}")
    loaded_model.summary()
    return loaded_model


def get_model_from_mlflow(experiment_name: str, run_id: str, artifact_uri: str):
    client = bigquery.Client()

    # get artifact_uri from BQ
    if artifact_uri is None or len(artifact_uri) <= 10:
        if run_id is None or len(run_id) <= 2:
            results_array = (
                client.query(
                    f"""
                    SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}`
                    WHERE experiment_name = '{experiment_name}'
                    ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        else:
            results_array = (
                client.query(
                    f"""
                    SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}`
                    WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}'
                    ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        if len(results_array) == 0:
            raise ValueError(
                f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}"
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
    for line in results.stdout:
        logger.info(line.rstrip().decode("utf-8"))
