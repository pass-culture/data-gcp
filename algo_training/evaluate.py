import numpy as np
import pandas as pd
import tensorflow as tf
import mlflow.tensorflow
import random
from models.v1.match_model import MatchModel
from utils import (
    get_secret,
    connect_remote_mlflow,
    STORAGE_PATH,
    ENV_SHORT_NAME,
    MODEL_NAME,
    RECOMMENDATION_NUMBER,
    EVALUATION_USER_NUMBER,
)
from metrics import compute_metrics, get_actual_and_predicted

k_list = [10, RECOMMENDATION_NUMBER]


def evaluate(model, storage_path: str, model_name):

    positive_data_train = pd.read_csv(
        f"{storage_path}/positive_data_train.csv",
        dtype={"user_id": str, "item_id": str, "offer_subcategoryid": str},
    )

    positive_data_test = pd.read_csv(
        f"{storage_path}/positive_data_test.csv",
        dtype={"user_id": str, "item_id": str, "offer_subcategoryid": str},
    )

    positive_data_test_clean = positive_data_test[
        positive_data_test.user_id.isin(positive_data_train.user_id)
    ]

    # Extract random sub sample if len(users_to_evaluate) > EVALUATION_USER_NUMBER
    if len(positive_data_test_clean.user_id) > EVALUATION_USER_NUMBER:
        random_users_to_test = random.sample(
            positive_data_test_clean.user_id, EVALUATION_USER_NUMBER
        )

    positive_data_test_clean = positive_data_test_clean[
        positive_data_test_clean.user_id.isin(random_users_to_test)
    ]

    positive_data_test_clean = positive_data_test_clean[
        positive_data_test_clean.item_id.isin(positive_data_train.item_id)
    ]
    positive_data_test_clean = positive_data_test_clean.drop_duplicates()

    data_model_dict = {
        "name": model_name,
        "data": {"train": positive_data_train, "test": positive_data_test_clean},
        "model": model,
    }
    data_model_dict_w_actual_and_predicted = get_actual_and_predicted(data_model_dict)
    metrics = {}
    for k in k_list:
        data_model_dict_w_metrics_at_k = compute_metrics(
            data_model_dict_w_actual_and_predicted, k
        )

        metrics[f"recall_at_{k}"] = data_model_dict_w_metrics_at_k["metrics"]["mark"]
        metrics[f"precision_at_{k}"] = data_model_dict_w_metrics_at_k["metrics"]["mapk"]

        metrics[f"recall_at_{k}_div"] = data_model_dict_w_metrics_at_k["metrics"][
            "div_mark"
        ]
        metrics[f"precision_at_{k}_div"] = data_model_dict_w_metrics_at_k["metrics"][
            "div_mapk"
        ]
        metrics[f"coverage_at_{k}"] = (
            data_model_dict_w_metrics_at_k["metrics"]["coverage"],
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
    with mlflow.start_run(run_id=run_id):
        loaded_model = tf.keras.models.load_model(
            mlflow.get_artifact_uri("model"),
            custom_objects={"MatchModel": MatchModel},
            compile=False,
        )
        evaluate(loaded_model, STORAGE_PATH, MODEL_NAME)
