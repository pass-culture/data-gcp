import os
import mlflow

import numpy as np
import pandas as pd
import tensorflow as tf

from models.match_model import MatchModel
from models.triplet_model import TripletModel
from models.utils import identity_loss, sample_triplets, predict
from utils import get_secret, connect_remote_mlflow


STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
MLFLOW_EHP_URI = "https://mlflow-ehp.internal-passculture.app/"
MLFLOW_PROD_URI = "https://mlflow.internal-passculture.app/"

TRAIN_DIR = "/home/airflow/train"
EMBEDDING_SIZE = 64
L2_REG = 0
N_EPOCHS = 20
BATCH_SIZE = 32


def train(storage_path: str):

    bookings = pd.read_csv(
        f"{storage_path}/clean_data.csv", dtype={"user_id": str, "item_id": str}
    )

    pos_data_train = pd.read_csv(
        f"{storage_path}/pos_data_train.csv", dtype={"user_id": str, "item_id": str}
    )
    pos_data_eval = pd.read_csv(
        f"{storage_path}/pos_data_eval.csv", dtype={"user_id": str, "item_id": str}
    )

    user_ids = bookings["user_id"].unique().tolist()
    item_ids = bookings["item_id"].unique().tolist()

    experiment_name = "algo_training_v1"
    experiment = mlflow.get_experiment_by_name(experiment_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id):

        triplet_model = TripletModel(
            user_ids, item_ids, latent_dim=EMBEDDING_SIZE, l2_reg=L2_REG
        )
        match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)
        predict(match_model)

        fake_y = np.array(["1"] * pos_data_train["user_id"].shape[0], dtype=object)
        evaluation_fake_train = np.array(
            ["1"] * pos_data_eval["user_id"].shape[0], dtype=object
        )
        triplet_model.compile(loss=identity_loss, optimizer="adam")

        best_eval = 1

        runned_epochs = 0
        for i in range(N_EPOCHS):
            triplet_inputs = sample_triplets(pos_data_train, item_ids)
            evaluation_triplet_inputs = sample_triplets(pos_data_eval, item_ids)

            print(f"Training epoch {i}")
            train_result = triplet_model.fit(
                x=triplet_inputs,
                y=fake_y,
                shuffle=True,
                BATCH_SIZE=BATCH_SIZE,
                epochs=1,
                verbose=2,
            )
            mlflow.log_metric(
                key="Training Loss", value=train_result.history["loss"][0], step=i
            )

            print(f"Evaluation epoch {i}")
            eval_result = triplet_model.evaluate(
                x=evaluation_triplet_inputs,
                y=evaluation_fake_train,
                BATCH_SIZE=BATCH_SIZE,
            )
            mlflow.log_metric(key="Evaluation Loss", value=eval_result, step=i)

            runned_epochs += 1
            if eval_result < best_eval:
                best_eval = eval_result

                run_uuid = mlflow.active_run().info.run_uuid
                export_path = f"{TRAIN_DIR}/{run_uuid}"
                tf.saved_model.save(match_model, export_path)
                mlflow.log_artifacts(export_path, "model")


if __name__ == "__main__":
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    train(STORAGE_PATH)
    print("------- TRAINING DONE -------")
