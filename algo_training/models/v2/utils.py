import os

import mlflow

import numpy as np
import tensorflow as tf
import pandas as pd
from loguru import logger
from sklearn.decomposition import PCA
import matplotlib as mpl
import matplotlib.pyplot as plt

from metrics import get_actual_and_predicted, compute_metrics
from utils import connect_remote_mlflow


def load_triplets_dataset(
    input_data: pd.DataFrame,
    user_columns,
    item_columns,
    batch_size: int,
) -> tf.data.Dataset:
    """
    Builds a tf Dataset of shape with:
        - X = (user_id, the item_id associated to the user, a random item_id)
        - y = fake data as we make no predictions
    """

    anchor_data = list(zip(*[input_data[col] for col in user_columns]))
    positive_data = list(zip(*[input_data[col] for col in item_columns]))
    input_data = input_data.sample(frac=1)
    negative_data = list(zip(*[input_data[col] for col in item_columns]))

    anchor_dataset = tf.data.Dataset.from_tensor_slices(anchor_data)
    positive_dataset = tf.data.Dataset.from_tensor_slices(positive_data)
    negative_dataset = (
        tf.data.Dataset.from_tensor_slices(negative_data)
        .shuffle(buffer_size=len(positive_data))
        .repeat()
    )  # We shuffle the negative examples to get new random examples at each call

    x = tf.data.Dataset.zip((anchor_dataset, positive_dataset, negative_dataset))
    y = tf.data.Dataset.from_tensor_slices(np.ones((len(input_data), 3)))

    return tf.data.Dataset.zip((x, y)).batch(
        batch_size=batch_size, drop_remainder=False
    )


class MatchModelCheckpoint(tf.keras.callbacks.Callback):
    def __init__(self, match_model: tf.keras.models.Model, filepath: str):
        super(MatchModelCheckpoint, self).__init__()
        self.match_model = match_model
        self.filepath = filepath

    def on_epoch_end(self, epoch, logs=None):
        tf.keras.models.save_model(self.match_model, self.filepath + "model")


class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(self, client_id: str, env: str, export_path: str):
        super(MLFlowLogging, self).__init__()
        self.client_id = client_id
        self.env = env
        self.export_path = export_path

    def on_epoch_end(self, epoch, logs=None):
        connect_remote_mlflow(self.client_id, env=self.env)
        mlflow.log_metrics(
            {
                "Training Loss": logs["loss"],
                "Evaluation Loss": logs["val_loss"],
            },
            step=epoch,
        )

    def on_train_end(self, epoch, logs=None):
        connect_remote_mlflow(self.client_id, env=self.env)
        mlflow.log_artifacts(self.export_path + "model", "model")


def load_metrics(data_model_dict: dict, k_list: list, recommendation_number: int):
    data_model_dict_w_actual_and_predicted = get_actual_and_predicted(data_model_dict)
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
        if k == recommendation_number:
            metrics.update(
                {
                    f"precision_at_{k}_panachage": metrics_at_k["mapk_panachage"],
                    f"recall_at_{k}_panachage": metrics_at_k["mark_panachage"],
                    f"avg_diversification_score_at_{k}": metrics_at_k["avg_div_score"],
                    f"avg_diversification_score_at_{k}_panachage": metrics_at_k[
                        "avg_div_score_panachage"
                    ],
                    f"personalization_at_{k}_panachage": metrics_at_k[
                        "personalization_at_k_panachage"
                    ],
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
