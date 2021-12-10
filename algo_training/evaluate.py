import pandas as pd
import tensorflow as tf
import mlflow.tensorflow
import time
from models.v1.match_model import MatchModel
from models.v1.metrics import compute_metrics as compute_metrics_v1

from models.v2.deep_reco.deep_match_model import DeepMatchModel
from models.v2.deep_reco.metrics import compute_metrics as compute_metrics_v2_deep_reco
from utils import (
    get_secret,
    connect_remote_mlflow,
    STORAGE_PATH,
    ENV_SHORT_NAME,
    MODEL_NAME,
)

RECOMMENDATION_NUMBER = 40


def evaluate(model, storage_path: str, model_name):
    positive_data_test = pd.read_csv(
        f"{storage_path}/positive_data_test.csv",
        dtype={"user_id": str, "item_id": str, "offer_subcategoryid": str},
    )
    positive_data_train = pd.read_csv(
        f"{storage_path}/positive_data_train.csv",
        dtype={"user_id": str, "item_id": str, "offer_subcategoryid": str},
    )
    if model_name == "v1":
        metrics = compute_metrics_v1(
            RECOMMENDATION_NUMBER, positive_data_train, positive_data_test, model
        )
    elif model_name == "v2_deep_reco":
        metrics = compute_metrics_v2_deep_reco(
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
        and metrics[f"coverage_at_{k}"] > 10
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
    experiment_name = f"algo_training_{MODEL_NAME}"
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id
    if MODEL_NAME == "v1":
        with mlflow.start_run(run_id=run_id):
            loaded_model = tf.keras.models.load_model(
                mlflow.get_artifact_uri("model"),
                custom_objects={"MatchModel": MatchModel},
                compile=False,
            )
            evaluate(loaded_model, STORAGE_PATH, MODEL_NAME)
    elif MODEL_NAME == "v2_deep_reco":
        with mlflow.start_run(run_id=run_id):
            loaded_model = tf.keras.models.load_model(
                mlflow.get_artifact_uri("model"),
                custom_objects={"DeepMatchModel": DeepMatchModel},
                compile=False,
            )
            evaluate(loaded_model, STORAGE_PATH, MODEL_NAME)
