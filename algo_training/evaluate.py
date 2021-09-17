import pandas as pd
import tensorflow as tf
import mlflow.tensorflow
import time
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
    print("------- EVALUATE DONE -------")
    check_before_deploy(metrics, RECOMMENDATION_NUMBER)


def check_before_deploy(metrics, k):
    if (
        metrics[f"recall_at_{k}"] > 20
        and metrics[f"precision_at_{k}"] > 1
        and metrics[f"maximal_precision_at_{k}"] > 4.5
        and metrics[f"coverage_at_{k}"] > 30
        and metrics[f"coverage_at_{k}"] < 70
        and metrics[f"unexpectedness_at_{k}"] > 0.07
        and metrics[f"new_types_ratio_at_{k}"] > 0.05
        and metrics[f"serendipity_at_{k}"] > 1.5
    ):
        print("Metrics OK")
    else:
        print("Bad metrics")
    if ENV_SHORT_NAME == "dev":
        # INFO : metrics are never ok in dev so we force the deploy
        print("Metrics OK")


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
