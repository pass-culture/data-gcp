import mlflow
import random

import numpy as np
import tensorflow as tf
import pandas as pd
from sklearn.decomposition import PCA

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
        tf.keras.models.save_model(self.match_model, self.filepath + "/model")


class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(
        self, client_id: str, env: str, export_path: str, item_categories: pd.DataFrame
    ):
        super(MLFlowLogging, self).__init__()
        self.client_id = client_id
        self.env = env
        self.export_path = export_path
        self.item_categories = item_categories

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

        # We remove the first element, the [UNK] token
        item_ids = self.item_layer.layers[0].get_vocabulary()[1:]
        embeddings = self.item_layer.layers[1].get_weights()[0][1:]

        pca_out = PCA(n_components=2).fit_transform(embeddings)

        item_representation = pd.DataFrame(
            {
                "item_id": item_ids,
                "x": pca_out[:, 0],
                "y": pca_out[:, 1],
                "offer_categoryId": self.item_categories.loc[item_ids]["offer_categoryId"].values
            }
        )

        fig = item_representation.plot.scatter(
            x="x", y="y", c="offer_categoryId", colormap="tab20"
        ).get_figure()
        fig.savefig(self.export_path + "embedding_representation.pdf")
        mlflow.log_artifact(
            self.export_path + "embedding_representation.pdf",
            "embedding_representation.png",
        )

        remove_dir(self.export_path)
