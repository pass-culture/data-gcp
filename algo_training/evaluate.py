import pandas as pd
import tensorflow as tf
import mlflow.tensorflow
from models.match_model import MatchModel

from models.metrics import compute_metrics
from utils import get_secret, connect_remote_mlflow, STORAGE_PATH, ENV_SHORT_NAME

RECOMMENDATION_NUMBER = 40


def evaluate(model, storage_path: str):
    positive_data_test = pd.read_csv(
        f"{storage_path}/positive_data_test.csv", dtype={"user_id": str, "item_id": str}
    )
    positive_data_train = pd.read_csv(
        f"{storage_path}/positive_data_train.csv",
        dtype={"user_id": str, "item_id": str},
    )

    metrics = compute_metrics(
        RECOMMENDATION_NUMBER, positive_data_train, positive_data_test, model
    )
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    mlflow.log_metrics(metrics)


if __name__ == "__main__":
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)

    experiment_name = "algo_training_v1"
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id
    with mlflow.start_run(run_id=run_id):
        loaded_model = tf.keras.models.load_model(
            mlflow.get_artifact_uri("model"), custom_objects={"MatchModel": MatchModel}
        )
        evaluate(loaded_model, STORAGE_PATH)
    print("------- EVALUATE DONE -------")
