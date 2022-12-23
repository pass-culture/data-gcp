import mlflow
import random

import numpy as np
import tensorflow as tf
import pandas as pd
from sklearn.decomposition import PCA

from metrics import get_actual_and_predicted, compute_metrics
from utils import connect_remote_mlflow, remove_dir


def load_triplets_dataset(
    dataset: pd.DataFrame,
    item_ids: list,
    seed: int,
) -> tf.data.Dataset:
    """
    Builds a tf Dataset of shape with:
        - X = (user_id, the item_id associated to the user, a random item_id)
        - y = fake data as we make no predictions
    """

    random.seed(seed)
    dataset = dataset.sample(frac=1, random_state=seed).reset_index(drop=True)

    anchor_data = dataset["user_id"].values
    positive_data = dataset["item_id"].values
    negative_data = random.choices(item_ids, k=len(dataset))

    return tf.data.Dataset.from_tensor_slices(
        (
            np.column_stack((anchor_data, positive_data, negative_data)),
            np.ones((len(dataset), 3)),
        )
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
        remove_dir(self.export_path)


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


def save_pca_representation(
    loaded_model: tf.keras.models.Model,
    training_item_categories: pd.DataFrame,
    figure_path: str,
):
    # We remove the first element, the [UNK] token
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    pca_out = PCA(n_components=2).fit_transform(embeddings)

    item_representation = pd.DataFrame(
        {
            "item_id": item_ids,
            "x": pca_out[:, 0],
            "y": pca_out[:, 1],
            "offer_categoryId": training_item_categories.loc[item_ids][
                "offer_categoryId"
            ].values,
        }
    )

    fig = item_representation.plot.scatter(
        x="x", y="y", c="offer_categoryId", colormap="tab20"
    ).get_figure()
    fig.savefig()
