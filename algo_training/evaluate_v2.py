import tensorflow as tf
import mlflow.tensorflow

import typer

from tools.data_collect_queries import get_data, get_column_data
from models.v1.match_model import MatchModel
from utils import (
    get_secret,
    connect_remote_mlflow,
    ENV_SHORT_NAME,
    RECOMMENDATION_NUMBER,
    NUMBER_OF_PRESELECTED_OFFERS,
    EVALUATION_USER_NUMBER,
)
from metrics import compute_metrics, get_actual_and_predicted

k_list = [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]


def evaluate(
    experiment_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    )
):

    booking_raw_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="training_data_bookings"
    )

    training_item_ids = get_column_data(
        dataset=f"raw_{ENV_SHORT_NAME}",
        table_name="recommendation_training_data",
        column_name="item_id",
    ).values

    test_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="recommendation_test_data"
    )

    # We test maximum EVALUATION_USER_NUMBER users
    users_to_test = test_data["user_id"].unique()[
        : min(EVALUATION_USER_NUMBER, test_data["user_id"].nunique())
    ]
    test_data = test_data.loc[lambda df: df["user_id"].isin(users_to_test)]

    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id
    with mlflow.start_run(run_id=run_id):
        loaded_model = tf.keras.models.load_model(
            mlflow.get_artifact_uri("model"),
            custom_objects={"MatchModel": MatchModel},
            compile=False,
        )

        data_model_dict = {
            "name": experiment_name,
            "data": {
                "raw": booking_raw_data,
                "training_item_ids": training_item_ids,
                "test": test_data,
            },
            "model": loaded_model,
        }

        data_model_dict_w_actual_and_predicted = get_actual_and_predicted(
            data_model_dict
        )
        metrics = {}
        for k in k_list:
            metrics_at_k = compute_metrics(data_model_dict_w_actual_and_predicted, k)[
                "metrics"
            ]

            metrics.update(
                {
                    f"precision_at_{k}": metrics_at_k["mapk"],
                    f"recall_at_{k}": metrics_at_k["mark"],
                    f"coverage_at_{k}": metrics_at_k["coverage"],
                    f"personalization_at_{k}": metrics_at_k["personalization_at_k"],
                }
            )

            # Here we track metrics relate to pcreco output
            if k == RECOMMENDATION_NUMBER:
                metrics.update(
                    {
                        f"precision_at_{k}_panachage": metrics_at_k["mapk_panachage"],
                        f"recall_at_{k}_panachage": metrics_at_k["mark_panachage"],
                        f"avg_diversification_score_at_{k}": metrics_at_k[
                            "avg_div_score"
                        ],
                        f"avg_diversification_score_at_{k}_panachage": metrics_at_k[
                            "avg_div_score_panachage"
                        ],
                        f"personalization_at_{k}_panachage": metrics_at_k[
                            "personalization_at_k_panachage"
                        ],
                    }
                )

        mlflow.log_metrics(metrics)
        print("------- EVALUATE DONE -------")


if __name__ == "__main__":
    typer.run(evaluate)
