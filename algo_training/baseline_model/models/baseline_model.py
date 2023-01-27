import tensorflow as tf
import tensorflow_recommenders as tfrs
import pandas as pd
import numpy as np

from utils.layers import (
    StringEmbeddingLayer,
)


class BaselineModel(tfrs.models.Model):
    def __init__(
        self,
        user_ids: np.ndarray,
        item_ids: np.ndarray,
        embedding_size: int,
    ):
        """
        user_ids: List containing the ids for each user
        item_ids: List containing the ids for each item
        embedding_size: Final embedding size for the items & the users
        """
        super().__init__()

        self.user_model = StringEmbeddingLayer(
            embedding_size=embedding_size
        ).build_sequential_layer(vocabulary=user_ids)
        self.item_model = StringEmbeddingLayer(
            embedding_size=embedding_size
        ).build_sequential_layer(vocabulary=item_ids)

        item_ids_dataset = tf.data.Dataset.from_tensor_slices(item_ids).batch(
            batch_size=1024, drop_remainder=False
        )

        self.task = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=item_ids_dataset.map(self.item_model),
            ),
        )

    def compute_loss(self, inputs, training=False):
        user_id, item_id = inputs
        user_embedding = self.user_model(user_id)
        item_embedding = self.item_model(item_id)

        return self.task(user_embedding, item_embedding, compute_metrics=not training)
