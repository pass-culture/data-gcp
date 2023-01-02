import mlflow

import numpy as np
import tensorflow as tf
import pandas as pd
from sklearn.decomposition import PCA

from metrics import get_actual_and_predicted, compute_metrics
from utils import connect_remote_mlflow


def load_triplets_dataset(
    input_data: pd.DataFrame,
    item_ids: list,
    batch_size: int,
) -> tf.data.Dataset:
    """
    Builds a tf Dataset of shape with:
        - X = (user_id, the item_id associated to the user, a random item_id)
        - y = fake data as we make no predictions
    """

    anchor_data = input_data["user_id"].values
    positive_data = input_data["item_id"].values

    anchor_dataset = tf.data.Dataset.from_tensor_slices(anchor_data)
    positive_dataset = tf.data.Dataset.from_tensor_slices(positive_data)
    negative_dataset = (
        tf.data.Dataset.from_tensor_slices(item_ids)
        .shuffle(buffer_size=len(item_ids))
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
    training_item_categories: pd.DataFrame,
    figures_folder: str,
):
    # We remove the first element, the [UNK] token
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    pca_out = PCA(n_components=2).fit_transform(embeddings)

    categories = training_item_categories["offer_categoryId"].unique().tolist()
    item_representation = (
        pd.DataFrame(
            {
                "item_id": item_ids,
                "x": pca_out[:, 0],
                "y": pca_out[:, 1],
            }
        )
        .merge(training_item_categories, on=["item_id"], how="left")
        .assign(
            category_index=lambda df: df["offer_categoryId"].apply(
                lambda x: categories.index(x)
            )
        )
    )

    fig = item_representation.plot.scatter(
        x="x", y="y", c="category_index", colormap="tab20"
    ).get_figure()
    fig.savefig(figures_folder + f"ALL_CATEGORIES.pdf")
    for category in categories:
        fig = (
            item_representation.loc[lambda df: df["offer_categoryId"] == category]
            .plot.scatter(x="x", y="y", c="category_index", colormap="tab20")
            .get_figure()
        )
        fig.savefig(figures_folder + f"{category}.pdf")
