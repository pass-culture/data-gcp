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
from utils.data_collect_queries import read_from_gcs


from sklearn.decomposition import PCA
import matplotlib as mpl
import matplotlib.pyplot as plt


k_list = [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]


def evaluate(
    model: tf.keras.models.Model,
    storage_path: str,
    training_dataset_name: str = "recommendation_training_data",
    test_dataset_name: str = "recommendation_test_data",
):
    logger.info("Load raw")
    raw_data = read_from_gcs(storage_path, "bookings", read_from_gcs=False).astype(
        {"user_id": str, "item_id": str, "count": int}
    )
    logger.info(f"raw_data : {raw_data.shape[0]}")

    training_item_ids = read_from_gcs(
        storage_path, training_dataset_name, read_from_gcs=False
    )["item_id"].unique()
    logger.info(f"training_item_ids : {training_item_ids.shape[0]}")

    logger.info("Load training")
    positive_data_test = (
        read_from_gcs(
            storage_path,
            test_dataset_name,
        )
        .astype(
            {
                "user_id": str,
                "item_id": str,
            }
        )[["user_id", "item_id"]]
        .merge(raw_data, on=["user_id", "item_id"], how="inner")
        .drop_duplicates()
    )
    logger.info(f"positive_data_test : {training_item_ids.shape[0]}")

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


def save_pca_representation(
    loaded_model: tf.keras.models.Model,
    item_data: pd.DataFrame,
    figures_folder: str,
):
    # We remove the first element, the [UNK] token
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    pca_out = PCA(n_components=2).fit_transform(embeddings)
    categories = item_data["offer_categoryId"].unique().tolist()
    item_representation = pd.DataFrame(
        {
            "item_id": item_ids,
            "x": pca_out[:, 0],
            "y": pca_out[:, 1],
        }
    ).merge(item_data, on=["item_id"], how="inner")

    colormap = mpl.colormaps["tab20"].colors
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    for idx, category in enumerate(categories):
        data = item_representation.loc[lambda df: df["offer_categoryId"] == category]
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
        for idx_sub, subcategory in enumerate(data["offer_subcategoryid"].unique()):
            data_sub = data.loc[lambda df: df["offer_subcategoryid"] == subcategory]
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
