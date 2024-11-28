import json
import random

import matplotlib.pyplot as plt
import pandas as pd
import tensorflow as tf
from loguru import logger
from sklearn.decomposition import PCA

from commons.constants import (
    CONFIGS_PATH,
    EVALUATION_USER_NUMBER,
    EVALUATION_USER_NUMBER_DIVERSIFICATION,
    MODEL_DIR,
    NUMBER_OF_PRESELECTED_OFFERS,
    RECOMMENDATION_NUMBER,
)
from commons.data_collect_queries import read_from_gcs
from two_towers_model.utils.metrics import (
    compute_diversification_score,
    compute_metrics,
    get_actual_and_predicted,
)


def evaluate(
    model: tf.keras.models.Model,
    storage_path: str,
    training_dataset_name: str = "recommendation_training_data",
    test_dataset_name: str = "recommendation_test_data",
    config_file_name: str = "user-qpi-features",
):
    try:
        with open(
            f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
            mode="r",
            encoding="utf-8",
        ) as config_file:
            features = json.load(config_file)
            prediction_input_feature = features.get(
                "input_prediction_feature", "user_id"
            )
    except Exception:
        logger.info("Config file not found: setting default configuration")
        prediction_input_feature = "user_id"

    logger.info("Load raw data")
    raw_data = read_from_gcs(storage_path, "bookings", parallel=False).astype(
        {"user_id": str, "item_id": str}
    )
    logger.info(f"raw_data: {raw_data.shape[0]}")

    training_item_ids = read_from_gcs(
        storage_path, training_dataset_name, parallel=False
    )["item_id"].unique()
    logger.info(f"training_item_ids: {training_item_ids.shape[0]}")

    logger.info("Load test data")
    test_columns = [prediction_input_feature, "item_id"]
    if "user_id" not in test_columns:
        test_columns.append("user_id")
    positive_data_test = read_from_gcs(
        storage_path, test_dataset_name, parallel=False
    ).astype({"user_id": str, "item_id": str})[test_columns]
    logger.info("Merge all data")
    positive_data_test = positive_data_test.merge(
        raw_data, on=["user_id", "item_id"], how="inner"
    ).drop_duplicates()
    logger.info(f"positive_data_test: {positive_data_test.shape[0]}")

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
        "prediction_input_feature": prediction_input_feature,
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
    for k in [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]:
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

        if k == RECOMMENDATION_NUMBER:
            logger.info("Compute diversification score")
            avg_div_score, avg_div_score_panachage = compute_diversification_score(
                diversification_model_dict, k
            )
            logger.info("End of diversification score computation")

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


def save_pca_representation(
    loaded_model: tf.keras.models.Model,
    item_data: pd.DataFrame,
    figures_folder: str,
):
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    seed = random.randint(0, 1000)
    logger.info(f"Random state for PCA fixed to seed={seed}")
    pca_out = PCA(n_components=2, random_state=seed).fit_transform(embeddings)
    categories = item_data["offer_category_id"].unique().tolist()
    item_representation = pd.DataFrame(
        {
            "item_id": item_ids,
            "x": pca_out[:, 0],
            "y": pca_out[:, 1],
        }
    ).merge(item_data, on=["item_id"], how="inner")

    colormap = plt.cm.tab20.colors
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    for idx, category in enumerate(categories):
        data = item_representation.loc[lambda df: df["offer_category_id"] == category]
        max_plots = min(data.shape[0], 10000)
        data = data.sample(n=max_plots)
        ax.scatter(
            data["x"].values,
            data["y"].values,
            s=10,
            color=colormap[idx],
            label=category,
            alpha=0.7,
        )
        logger.info(f"Plotting {len(data)} points for category {category}")
        fig_sub, ax_sub = plt.subplots(1, 1, figsize=(15, 10))
        for idx_sub, subcategory in enumerate(data["offer_subcategory_id"].unique()):
            data_sub = data.loc[lambda df: df["offer_subcategory_id"] == subcategory]
            ax_sub.scatter(
                data_sub["x"].values,
                data_sub["y"].values,
                s=10,
                color=colormap[idx_sub],
                label=subcategory,
                alpha=0.7,
            )
        ax_sub.legend()
        ax_sub.grid(True)
        fig_sub.savefig(figures_folder + f"{category}.pdf")

    ax.legend()
    ax.grid(True)
    fig.savefig(figures_folder + "ALL_CATEGORIES.pdf")
