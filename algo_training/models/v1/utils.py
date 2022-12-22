import random
from typing import Tuple

import mlflow
import numpy as np
import pandas as pd
import tensorflow as tf

from utils import connect_remote_mlflow, remove_dir


def identity_loss(y_true, y_pred):
    """Ignore y_true and return the mean of y_pred

    This is a hack to work-around the design of the Keras API that is
    not really suited to train networks with a triplet loss by default.
    """
    return tf.reduce_mean(y_pred)


def sample_triplets(positive_data, item_ids):
    """Sample negatives at random"""

    user_ids = positive_data["user_id"].values
    positive_item_ids = positive_data["item_id"].values
    negative__item_ids = np.array(
        random.choices(item_ids, k=len(user_ids)), dtype=object
    )

    return [user_ids, positive_item_ids, negative__item_ids]


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
        tf.keras.models.save_model(self.match_model, self.filepath)


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
        mlflow.log_artifacts(self.export_path, "model")
        remove_dir(self.export_path)


def predict(match_model):
    user_id = "19373"
    items_to_rank = np.array(
        ["offer-7514002", "product-2987109", "offer-6406524", "toto", "tata"]
    )
    repeated_user_id = np.empty_like(items_to_rank)
    repeated_user_id.fill(user_id)
    predicted = match_model.predict([repeated_user_id, items_to_rank], batch_size=4096)
    return predicted
