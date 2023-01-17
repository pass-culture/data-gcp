from typing import Any

import tensorflow as tf
import pandas as pd
import mlflow
import os
import shutil

from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import id_token

from utils.constants import GCP_PROJECT_ID, MLFLOW_PROD_URI, MLFLOW_EHP_URI


def build_dict_dataset(
    data: pd.DataFrame, feature_names: list, batch_size: int, seed: int = None
):
    return (
        tf.data.Dataset.from_tensor_slices(data.values)
        .map(lambda x: {column: x[idx] for idx, column in enumerate(feature_names)})
        .batch(batch_size=batch_size, drop_remainder=False)
        .shuffle(buffer_size=10 * batch_size, seed=seed, reshuffle_each_iteration=False)
    )


def fill_na_by_feature_type(df: pd.DataFrame, columns_to_fill: list, fill_value: Any):
    return df.fillna({col: fill_value for col in columns_to_fill})


def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow(client_id, env="ehp"):
    """
    Use this function to connect to the mlflow remote server.

    client_id : the oauth iap client id (in 1password)
    """
    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_PROD_URI if env == "prod" else MLFLOW_EHP_URI
    mlflow.set_tracking_uri(uri)


def remove_dir(path):
    shutil.rmtree(path)


class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(self, client_id: str, env: str, export_path: str):
        super(MLFlowLogging, self).__init__()
        self.client_id = client_id
        self.env = env
        self.export_path = export_path

    def on_epoch_end(self, epoch, logs=None):
        connect_remote_mlflow(self.client_id, env=self.env)
        mlflow.log_metrics(
            {
                "loss": logs["loss"],
                "val_loss": logs["val_loss"],
                "val_top_5_categorical_accuracy": logs[
                    "val_factorized_top_k/top_5_categorical_accuracy"
                ],
                "val_top_10_categorical_accuracy": logs[
                    "val_factorized_top_k/top_10_categorical_accuracy"
                ],
                "val_top_50_categorical_accuracy": logs[
                    "val_factorized_top_k/top_50_categorical_accuracy"
                ],
                "val_top_100_categorical_accuracy": logs[
                    "val_factorized_top_k/top_100_categorical_accuracy"
                ],
            },
            step=epoch,
        )
