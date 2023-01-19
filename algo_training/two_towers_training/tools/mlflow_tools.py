import os

import mlflow
import tensorflow as tf

from google.auth.transport.requests import Request
from google.cloud import secretmanager
from google.oauth2 import id_token

from tools.constants import GCP_PROJECT_ID, MLFLOW_EHP_URI, MLFLOW_PROD_URI


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
                "_train_loss": logs["loss"],
                "_val_loss": logs["val_loss"],
                "_val_top_5_categorical_accuracy": logs[
                    "val_factorized_top_k/top_5_categorical_accuracy"
                ],
                "_val_top_10_categorical_accuracy": logs[
                    "val_factorized_top_k/top_10_categorical_accuracy"
                ],
                "_val_top_50_categorical_accuracy": logs[
                    "val_factorized_top_k/top_50_categorical_accuracy"
                ],
                "_val_top_100_categorical_accuracy": logs[
                    "val_factorized_top_k/top_100_categorical_accuracy"
                ],
            },
            step=epoch,
        )
