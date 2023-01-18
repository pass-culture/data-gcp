import pandas as pd
import tensorflow as tf
import mlflow.tensorflow

from loguru import logger
from datetime import datetime

from models.match_model import TwoTowersMatchModel
from tools.mlflow_tools import (
    get_secret,
    connect_remote_mlflow,
)
from tools.constants import (
    STORAGE_PATH,
    ENV_SHORT_NAME,
    BIGQUERY_CLEAN_DATASET,
    MODELS_RESULTS_TABLE_NAME,
    GCP_PROJECT_ID,
    SERVING_CONTAINER,
    RECOMMENDATION_NUMBER,
    MODEL_NAME,
    NUMBER_OF_PRESELECTED_OFFERS,
    EVALUATION_USER_NUMBER,
    EXPERIMENT_NAME,
)
from tools.metrics import compute_metrics, get_actual_and_predicted

k_list = [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]


def evaluate(client_id, model, storage_path: str):

    raw_data = pd.read_csv(f"{storage_path}/bookings_data.csv").astype(
        {"user_id": "str", "item_id": "str", "count": "int"}
    )

    training_item_ids = pd.read_csv(f"{storage_path}/positive_data_train.csv")[
        "item_id"
    ].unique()

    positive_data_test = (
        pd.read_csv(
            f"{storage_path}/positive_data_test.csv",
            dtype={
                "user_id": str,
                "item_id": str,
            },
        )[["user_id", "item_id"]]
        .merge(raw_data, on=["user_id", "item_id"], how="inner")
        .drop_duplicates()
    )

    users_to_test = positive_data_test["user_id"].unique()[
        : min(EVALUATION_USER_NUMBER, positive_data_test["user_id"].nunique())
    ]
    positive_data_test = positive_data_test.loc[
        lambda df: df["user_id"].isin(users_to_test)
    ]

    data_model_dict = {
        "data": {
            "raw": raw_data,
            "training_item_ids": training_item_ids,
            "test": positive_data_test,
        },
        "model": model,
    }

    logger.info("Get predictions")
    data_model_dict_w_actual_and_predicted = get_actual_and_predicted(data_model_dict)
    metrics = {}
    for k in k_list:
        logger.info(f"Computing metrics for k={k}")
        data_model_dict_w_metrics_at_k = compute_metrics(
            data_model_dict_w_actual_and_predicted, k
        )

        metrics[f"recall_at_{k}"] = data_model_dict_w_metrics_at_k["metrics"]["mark"]
        metrics[f"precision_at_{k}"] = data_model_dict_w_metrics_at_k["metrics"]["mapk"]

        # Here we track metrics relate to pcreco output
        if k == RECOMMENDATION_NUMBER:
            metrics[f"recall_at_{k}_panachage"] = data_model_dict_w_metrics_at_k[
                "metrics"
            ]["mark_panachage"]
            metrics[f"precision_at_{k}_panachage"] = data_model_dict_w_metrics_at_k[
                "metrics"
            ]["mapk_panachage"]

            # AVG diverisification score is only calculate at k=RECOMMENDATION_NUMBER to match pcreco output
            metrics[
                f"avg_diversification_score_at_{k}"
            ] = data_model_dict_w_metrics_at_k["metrics"]["avg_div_score"]

            metrics[
                f"avg_diversification_score_at_{k}_panachage"
            ] = data_model_dict_w_metrics_at_k["metrics"]["avg_div_score_panachage"]

            metrics[
                f"personalization_at_{k}_panachage"
            ] = data_model_dict_w_metrics_at_k["metrics"][
                "personalization_at_k_panachage"
            ]

        metrics[f"coverage_at_{k}"] = data_model_dict_w_metrics_at_k["metrics"][
            "coverage"
        ]

        metrics[f"personalization_at_{k}"] = data_model_dict_w_metrics_at_k["metrics"][
            "personalization_at_k"
        ]

    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    mlflow.log_metrics(metrics)
    print("------- EVALUATE DONE -------")
    return metrics


def run(experiment_name: str, model_name: str):
    logger.info("-------EVALUATE START------- ")
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id

    with mlflow.start_run(run_id=run_id) as run:
        artifact_uri = mlflow.get_artifact_uri("model")
        loaded_model = tf.keras.models.load_model(
            artifact_uri,
            custom_objects={"MatchModel": TwoTowersMatchModel},
            compile=False,
        )
        log_results = {
            "execution_date": datetime.now().isoformat(),
            "experiment_name": experiment_name,
            "model_name": model_name,
            "model_type": "tensorflow",
            "run_id": run_id,
            "run_start_time": run.info.start_time,
            "run_end_time": run.info.start_time,
            "artifact_uri": artifact_uri,
            "serving_container": SERVING_CONTAINER,
        }

        pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
            f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
            project_id=f"{GCP_PROJECT_ID}",
            if_exists="append",
        )
        evaluate(client_id, loaded_model, STORAGE_PATH)


if __name__ == "__main__":
    run(experiment_name=EXPERIMENT_NAME, model_name=MODEL_NAME)
