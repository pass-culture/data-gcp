import os
import subprocess

import numpy as np
import pandas as pd
import tensorflow as tf
from google.cloud import bigquery

from constants import (
    BIGQUERY_CLEAN_DATASET,
    MODEL_BASE_PATH,
    MODELS_RESULTS_TABLE_NAME,
)


def fetch_user_item_data_with_embeddings(config):
    user_data_df = fetch_or_load_data(type="user")
    item_data_df = fetch_or_load_data(type="item")
    # user_data_df = pd.read_parquet(f"{config['data_path']}/user_data.parquet")
    # item_data_df = pd.read_parquet(f"{config['data_path']}/item_data.parquet")
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
    print("Two Tower model loaded.")

    print("\nExtracting user and item embeddings...")
    # Get user and item embeddings from TT
    user_embedding_dict, item_embedding_dict = extract_embeddings(loaded_model)

    # Filter user IDs to those present in the model's user embedding vocabulary
    print("Filtering IDs based on model's embedding vocabulary...")
    user_set = set(user_embedding_dict.keys())
    user_id_list = [uid for uid in user_id_list if uid in user_set]
    user_id_list = user_id_list[: config["number_of_ids"]]
    print("len(user_id_list):", len(user_id_list))
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
            experiment_name=config["source_experiment_name"]["prod"],
            run_id=None,
            artifact_uri=None,
        )
        print(f"Model artifact_uri: {source_artifact_uri}")
        download_model(artifact_uri=source_artifact_uri)
    else:
        print(f"Model already present in {MODEL_BASE_PATH}, skipping download.")
    print("Model downloaded.")
    print("Model directory contents:")
    model_files = os.listdir(MODEL_BASE_PATH)
    print("\n".join(model_files))

    print("\nLoad Two Tower model...")
    saved_model_path = os.path.join(os.getcwd(), MODEL_BASE_PATH)
    print(f"Loading model from: {saved_model_path}")
    loaded_model = tf.keras.models.load_model(saved_model_path)
    print(f"Model loaded successfully from: {saved_model_path}")
    loaded_model.summary()
    return loaded_model


# from loguru import logger
def fetch_or_load_data(type: str) -> pd.DataFrame:
    if os.path.exists(f"data/{type}_data.parquet"):
        print(f"Loading existing data from data/{type}_data.parquet...")
        data_df = pd.read_parquet(f"data/{type}_data.parquet")
    else:
        print(f"Fetching data for {type}...")
        data = get_data_from_sql_query(f"sql/{type}_data.sql")
        data_df = pd.DataFrame(data)
        data_df.to_parquet(f"data/{type}_data.parquet", index=False)
    return data_df


def read_sql_query(file_path: str) -> str:
    """Read SQL query from a file and return as a string."""
    with open(file_path) as file:
        return file.read()


def get_data_from_sql_query(file_path: str) -> pd.DataFrame:
    """Fetch from BigQuery and return as DataFrame."""
    query = read_sql_query(file_path)
    client = bigquery.Client(project="passculture-data-prod")
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df


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
            raise Exception(
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
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))
