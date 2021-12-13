import mlflow
import numpy as np
import pandas as pd
import tensorflow as tf

from models.v2.deep_reco.deep_match_model import DeepMatchModel
from models.v2.deep_reco.deep_triplet_model import DeepTripletModel
from models.v2.deep_reco.utils import (
    identity_loss,
    sample_triplets,
    predict,
    mask_random,
)
from utils import (
    get_secret,
    connect_remote_mlflow,
    STORAGE_PATH,
    ENV_SHORT_NAME,
    BOOKING_DAY_NUMBER,
)

TRAIN_DIR = "/home/airflow/train"
EMBEDDING_SIZE = 64
L2_REG = 0
N_EPOCHS = 20 if ENV_SHORT_NAME == "prod" else 10
LOSS_CUTOFF = 0.005
BATCH_SIZE = 512


def train(storage_path: str):

    clicks = pd.read_csv(
        f"{storage_path}/clean_data.csv", dtype={"user_id": str, "item_id": str}
    )

    clicks_train_light = pd.read_csv(
        f"{storage_path}/positive_data_train.csv",
        dtype={"user_id": str, "item_id": str},
    )
    clicks_test_light = pd.read_csv(
        f"{storage_path}/positive_data_test.csv", dtype={"user_id": str, "item_id": str}
    )
    # TRAIN
    user_ids = clicks_train_light["user_id"].unique().tolist()
    item_ids = clicks_train_light["item_id"].unique().tolist()

    n_users = len(user_ids)
    n_item = len(item_ids)

    subcategories = clicks.offer_subcategoryid.unique().tolist()

    experiment_name = "algo_training_v2_deep_reco"
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id):
        mlflow.set_tag("type", "prod_ready")
        hyper_parameters = dict(
            user_dim=32,
            item_dim=32,
            subcategories_dim=32,
            n_hidden=3,
            hidden_size=64,
            dropout=0.05,
            l2_reg=0.0,
            margin=1,
        )
        mlflow.log_params(hyper_parameters)

        mlflow.log_param("batch_size", BATCH_SIZE)

        deep_triplet_model = DeepTripletModel(
            user_ids, item_ids, subcategories, **hyper_parameters
        )
        deep_match_model = DeepMatchModel(
            deep_triplet_model.user_layer,
            deep_triplet_model.item_layer,
            deep_triplet_model.subcategory_layer,
            deep_triplet_model.mlp,
        )

        print("Predicted", predict(deep_match_model))

        deep_triplet_model.compile(loss=identity_loss, optimizer="adam")

        fake_y = np.array(["1"] * clicks_train_light["user_id"].shape[0], dtype=object)
        evaluation_fake_y = np.array(
            ["1"] * clicks_test_light["user_id"].shape[0], dtype=object
        )
        mlflow.log_param("n_epochs", N_EPOCHS)
        best_eval = 1
        for i in range(N_EPOCHS):
            connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
            print(f"Epoch {i}/{N_EPOCHS}")
            # Sample new negatives to build different triplets at each epoch
            triplet_inputs = sample_triplets(clicks_train_light, random_seed=i)
            triplet_inputs[0] = mask_random(triplet_inputs[0], proportion=0.01)

            evaluation_triplet_inputs = sample_triplets(
                clicks_test_light, random_seed=i
            )
            print("Debug_00: len(triplet_inputs)", len(triplet_inputs))
            print("Debug_01: len(fake_y)", len(fake_y))
            # Fit the model incrementally by doing a single pass over the
            # sampled triplets.
            train_result = deep_triplet_model.fit(
                triplet_inputs,
                fake_y,
                shuffle=True,
                batch_size=BATCH_SIZE,
                epochs=1,
                use_multiprocessing=True,
            )
            connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)

            mlflow.log_metric(
                key="Training Loss", value=train_result.history["loss"][0], step=i
            )

            print(f"Evaluation epoch {i}")
            eval_result = deep_triplet_model.evaluate(
                x=evaluation_triplet_inputs,
                y=evaluation_fake_y,
                batch_size=BATCH_SIZE * 2 * 2,
            )
            mlflow.log_metric(key="Evaluation Loss", value=eval_result, step=i)

            if eval_result < best_eval or i == 0:
                run_uuid = mlflow.active_run().info.run_uuid
                export_path = f"saved_model/prod_ready/{run_uuid}"
                tf.keras.models.save_model(deep_match_model, export_path)
                if ((best_eval - eval_result) / best_eval) < LOSS_CUTOFF and i != 0:
                    mlflow.log_param("Exit Epoch", i)
                    print("EXIT")
                    break
                else:
                    best_eval = eval_result
        connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
        mlflow.log_artifacts(export_path, "model")
        print("------- TRAINING DONE -------")
        print(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    train(STORAGE_PATH)
