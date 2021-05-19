import os
import json
import random
import mlflow

import numpy as np
import pandas as pd
import tensorflow as tf

from match_model import MatchModel
from margin_loss import MarginLoss

from google.cloud import secretmanager
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from tensorflow.keras import layers
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Input, Dense, Lambda, Dot
from tensorflow.keras.regularizers import l2
from triplet_model import TripletModel


def train(storage_path: str):
    TRAIN_DIR = "/home/airflow/train"

    bookings = pd.read_csv(
        f"{storage_path}/clean_data.csv", dtype={"user_id": str, "item_id": str}
    )

    pos_data_test = pd.read_csv(
        f"{storage_path}/pos_data_test.csv", dtype={"user_id": str, "item_id": str}
    )
    pos_data_train = pd.read_csv(
        f"{storage_path}/pos_data_train.csv", dtype={"user_id": str, "item_id": str}
    )
    pos_data_eval = pd.read_csv(
        f"{storage_path}/pos_data_eval.csv", dtype={"user_id": str, "item_id": str}
    )

    n_users = len(set(bookings.user_id.values))
    n_items = len(set(bookings.item_id.values))

    user_ids = bookings["user_id"].unique().tolist()
    item_ids = bookings["item_id"].unique().tolist()

    experiment_name = "algo_training_v1"
    experiment = mlflow.get_experiment_by_name(experiment_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id):
        EMBEDDING_SIZE = 64
        L2_REG = 0

        n_epochs = 20
        batch_size = 32

        triplet_model = TripletModel(
            user_ids, item_ids, latent_dim=EMBEDDING_SIZE, l2_reg=L2_REG
        )
        match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)

        fake_y = np.array(["1"] * pos_data_train["user_id"].shape[0], dtype=object)
        evaluation_fake_train = np.array(
            ["1"] * pos_data_eval["user_id"].shape[0], dtype=object
        )
        triplet_model.compile(loss=identity_loss, optimizer="adam")

        best_eval = 1

        train = []
        evaluation = []
        runned_epochs = 0
        for i in range(n_epochs):
            triplet_inputs = sample_triplets(pos_data_train, item_ids)
            evaluation_triplet_inputs = sample_triplets(pos_data_eval, item_ids)

            print(f"Training epoch {i}")
            train_result = triplet_model.fit(
                x=triplet_inputs,
                y=fake_y,
                shuffle=True,
                batch_size=64,
                epochs=1,
                verbose=2,
            )
            mlflow.log_metric(
                key="Train Loss", value=train_result.history["loss"][0], step=i
            )

            print(f"Evaluation epoch {i}")
            eval_result = triplet_model.evaluate(
                x=evaluation_triplet_inputs, y=evaluation_fake_train, batch_size=64
            )
            mlflow.log_metric(key="Evaluation Loss", value=eval_result, step=i)

            runned_epochs += 1
            if eval_result < best_eval:
                best_eval = eval_result

                run_uuid = mlflow.active_run().info.run_uuid
                export_path = TRAIN_DIR + "/" + run_uuid
                tf.saved_model.save(match_model, export_path)
                mlflow.log_artifacts(export_path, "model")


def identity_loss(y_true, y_pred):
    """Ignore y_true and return the mean of y_pred

    This is a hack to work-around the design of the Keras API that is
    not really suited to train networks with a triplet loss by default.
    """
    return tf.reduce_mean(y_pred)


def sample_triplets(pos_data, item_ids):
    """Sample negatives at random"""

    user_ids = pos_data["user_id"].values
    pos_item_ids = pos_data["item_id"].values
    neg_item_ids = np.array(random.choices(item_ids, k=len(user_ids)), dtype=object)

    return [user_ids, pos_item_ids, neg_item_ids]


def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow(client_id, env="ehp"):
    """
    Use this function to connect to the mlflow remote server.

    client_id : the oauth iap client id (in 1password)
    """

    MLFLOW_EHP_URI = "https://mlflow-ehp.internal-passculture.app/"
    MLFLOW_PROD_URI = "https://mlflow.internal-passculture.app/"

    client_id = get_secret("mlflow_client_id")

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_PROD_URI if env == "prod" else MLFLOW_EHP_URI
    mlflow.set_tracking_uri(uri)


def main():
    STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
    ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
    client_id = get_secret("mlflow_client_id", env=ENV_SHORT_NAME)
    connect_remote_mlflow(client_id)
    train(STORAGE_PATH)
    print("------- TRAINING DONE -------")


if __name__ == "__main__":
    main()
