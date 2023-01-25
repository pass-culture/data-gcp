import pandas as pd
from loguru import logger
import tensorflow as tf

from utils.metrics import (
    compute_metrics,
    get_actual_and_predicted,
    compute_diversification_score,
)
from utils.constants import (
    RECOMMENDATION_NUMBER,
    NUMBER_OF_PRESELECTED_OFFERS,
    EVALUATION_USER_NUMBER,
    EVALUATION_USER_NUMBER_DIVERSIFICATION,
)

k_list = [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]


def evaluate(
    model: tf.keras.models.Model,
    storage_path: str,
    training_dataset_name: str = "recommendation_training_data",
    test_dataset_name: str = "recommendation_test_data",
):
    raw_data = pd.read_csv(f"{storage_path}/bookings.csv").astype({"count": int})

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
    return metrics

