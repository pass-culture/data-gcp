from datetime import datetime

import mlflow.tensorflow
import pandas as pd
import tensorflow as tf
import typer
from loguru import logger

from metrics import (
    compute_metrics,
    get_actual_and_predicted,
    compute_diversification_score,
)
from models.v1.match_model import MatchModel
from tools.data_collect_queries import get_data
from utils import (
    get_secret,
    connect_remote_mlflow,
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
    EVALUATION_USER_NUMBER_DIVERSIFICATION,
)

k_list = [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]


def evaluate(
    client_id,
    model,
    storage_path: str,
    training_dataset_name: str = "positive_data_train",
    test_dataset_name: str = "positive_data_test",
):
    raw_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="training_data_bookings"
    )

    training_item_ids = pd.read_csv(f"{storage_path}/{training_dataset_name}.csv")[
        "item_id"
    ].unique()

    positive_data_test = (
        pd.read_csv(
            f"{storage_path}/{test_dataset_name}.csv",
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
    data_model_dict = {
        "data": {
            "raw": raw_data,
            "training_item_ids": training_item_ids,
            "test": positive_data_test.loc[
                lambda df: df["user_id"].isin(users_to_test)
            ],
        },
        "model": model,
    }

    diversification_users_to_test = users_to_test
    if input_type == "clicks":
        diversification_users_to_test = positive_data_test["user_id"].unique()[
            : min(
                EVALUATION_USER_NUMBER_DIVERSIFICATION,
                positive_data_test["user_id"].nunique(),
            )
        ]

    diversification_model_dict = {
        "data": {
            "raw": raw_data,
            "training_item_ids": training_item_ids,
            "test": positive_data_test.loc[
                lambda df: df["user_id"].isin(diversification_users_to_test)
            ],
        },
        "model": model,
    }

    logger.info("Get predictions")
    data_model_dict_w_actual_and_predicted = get_actual_and_predicted(data_model_dict)
    diversification_model_dict["top_offers"] = data_model_dict_w_actual_and_predicted[
        "top_offers"
    ].loc[lambda df: df["user_id"].isin(diversification_users_to_test)]

    metrics = {}
    for k in k_list:
        logger.info(f"Computing metrics for k={k}")
        data_model_dict_w_metrics_at_k = compute_metrics(
            data_model_dict_w_actual_and_predicted, k
        )

        metrics.update(
            {
                f"precision_at_{k}": data_model_dict_w_metrics_at_k["metrics"]["mapk"],
                f"recall_at_{k}": data_model_dict_w_metrics_at_k["metrics"]["mark"],
                f"coverage_at_{k}": data_model_dict_w_metrics_at_k["metrics"][
                    "coverage"
                ],
                f"personalization_at_{k}": data_model_dict_w_metrics_at_k["metrics"][
                    "personalization_at_k"
                ],
            }
        )

        # Here we track metrics relate to pcreco output
        if k == RECOMMENDATION_NUMBER:
            # AVG diverisification score is only calculate at k=RECOMMENDATION_NUMBER to match pcreco output
            logger.info("Compute diversification score")
            avg_div_score, avg_div_score_panachage = compute_diversification_score(
                diversification_model_dict, k
            )
            logger.info("End of diverisification score computation")

            metrics.update(
                {
                    f"precision_at_{k}_panachage": data_model_dict_w_metrics_at_k[
                        "metrics"
                    ]["mapk_panachage"],
                    f"recall_at_{k}_panachage": data_model_dict_w_metrics_at_k[
                        "metrics"
                    ]["mark_panachage"],
                    f"avg_diversification_score_at_{k}": avg_div_score,
                    f"avg_diversification_score_at_{k}_panachage": avg_div_score_panachage,
                    f"personalization_at_{k}_panachage": data_model_dict_w_metrics_at_k[
                        "metrics"
                    ]["personalization_at_k_panachage"],
                }
            )

    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    mlflow.log_metrics(metrics)
    print("------- EVALUATE DONE -------")
    return metrics


def run(
    experiment_name: str = typer.Option(
        EXPERIMENT_NAME, help="Name of the experiment on MLflow"
    ),
    model_name: str = typer.Option(MODEL_NAME, help="Name of the model to evaluate"),
    training_dataset_name: str = typer.Option(
        "positive_data_train", help="Name of the training dataset in storage"
    ),
    test_dataset_name: str = typer.Option(
        "positive_data_test", help="Name of the test dataset in storage"
    ),
):
    logger.info("-------EVALUATE START------- ")
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id

    with mlflow.start_run(run_id=run_id) as run:
        artifact_uri = mlflow.get_artifact_uri("model")
        loaded_model = tf.keras.models.load_model(
            artifact_uri,
            custom_objects={"MatchModel": MatchModel},
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
        metrics = evaluate(
            client_id,
            loaded_model,
            STORAGE_PATH,
            training_dataset_name,
            test_dataset_name,
        )


if __name__ == "__main__":
    typer.run(run)
