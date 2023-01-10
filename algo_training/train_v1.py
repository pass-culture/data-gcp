import mlflow
import warnings

from loguru import logger
from pandas.errors import DtypeWarning

from tools.logging_tools import log_memory_info

import numpy as np
import pandas as pd
import tensorflow as tf

from models.v1.match_model import MatchModel
from models.v1.triplet_model import TripletModel
from models.v1.utils import identity_loss, sample_triplets, predict
from utils import (
    get_secret,
    connect_remote_mlflow,
    remove_dir,
    STORAGE_PATH,
    ENV_SHORT_NAME,
    EXPERIMENT_NAME,
    TRAIN_DIR,
)

warnings.simplefilter(action="ignore", category=DtypeWarning)


EMBEDDING_SIZE = 64
L2_REG = 0
N_EPOCHS = 20 if ENV_SHORT_NAME == "prod" else 3
VERBOSE = 0 if ENV_SHORT_NAME == "prod" else 1
BATCH_SIZE = 1024
LOSS_CUTOFF = 0.005


def train(storage_path: str):

    bookings = pd.read_csv(
        f"{storage_path}/clean_data.csv", dtype={"user_id": str, "item_id": str}
    )

    positive_data_train = pd.read_csv(
        f"{storage_path}/positive_data_train.csv",
        dtype={"user_id": str, "item_id": str},
    )
    positive_data_eval = pd.read_csv(
        f"{storage_path}/positive_data_eval.csv", dtype={"user_id": str, "item_id": str}
    )

    user_ids = bookings["user_id"].unique().tolist()
    item_ids = bookings["item_id"].unique().tolist()

    experiment_name = EXPERIMENT_NAME
    experiment = mlflow.get_experiment_by_name(experiment_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id):

        triplet_model = TripletModel(
            user_ids, item_ids, latent_dim=EMBEDDING_SIZE, l2_reg=L2_REG
        )
        match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)
        predict(match_model)

        mlflow.log_param("environment", ENV_SHORT_NAME)
        mlflow.log_param("embedding_size", EMBEDDING_SIZE)
        mlflow.log_param("batch_size", BATCH_SIZE)
        mlflow.log_param("l2_regularization", L2_REG)
        mlflow.log_param("epoch_number", N_EPOCHS)
        mlflow.log_param("user_count", len(user_ids))
        mlflow.log_param("item_count", len(item_ids))

        fake_y = np.array(["1"] * positive_data_train["user_id"].shape[0], dtype=object)
        evaluation_fake_train = np.array(
            ["1"] * positive_data_eval["user_id"].shape[0], dtype=object
        )
        triplet_model.compile(loss=identity_loss, optimizer="adam")

        users_booked_items_train = (
            positive_data_train.groupby("user_id")["item_id"].apply(list).to_dict()
        )
        users_booked_items_eval = (
            positive_data_eval.groupby("user_id")["item_id"].apply(list).to_dict()
        )

        best_eval = 1
        runned_epochs = 0
        for i in range(N_EPOCHS):
            triplet_inputs = sample_triplets(
                positive_data_train, item_ids, users_booked_items_train
            )
            evaluation_triplet_inputs = sample_triplets(
                positive_data_eval, item_ids, users_booked_items_eval
            )

            print(f"Training epoch {i}")
            train_result = triplet_model.fit(
                x=triplet_inputs,
                y=fake_y,
                shuffle=True,
                batch_size=BATCH_SIZE,
                epochs=1,
                verbose=0,
            )
            connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
            mlflow.log_metric(
                key="Training Loss", value=train_result.history["loss"][0], step=i
            )

            print(f"Evaluation epoch {i}")
            eval_result = triplet_model.evaluate(
                x=evaluation_triplet_inputs,
                y=evaluation_fake_train,
                batch_size=2048,
                verbose=0,
            )
            connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
            mlflow.log_metric(key="Evaluation Loss", value=eval_result, step=i)

            runned_epochs += 1
            if eval_result < best_eval or runned_epochs == 1:
                run_uuid = mlflow.active_run().info.run_uuid
                export_path = f"{TRAIN_DIR}/{run_uuid}"
                tf.keras.models.save_model(match_model, export_path)
                if (
                    (best_eval - eval_result) / best_eval
                ) < LOSS_CUTOFF and runned_epochs != 1:
                    mlflow.log_param("Exit Epoch", runned_epochs)
                    break
                else:
                    best_eval = eval_result

            if VERBOSE:
                logger.info(log_memory_info())

        tf.keras.models.save_model(
            match_model, f"{TRAIN_DIR}/{ENV_SHORT_NAME}/tf_reco/"
        )
        connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
        mlflow.log_artifacts(export_path, "model")
        remove_dir(export_path)
        print("------- TRAINING DONE -------")
        print(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    train(STORAGE_PATH)
