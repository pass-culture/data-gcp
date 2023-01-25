import tensorflow as tf
import tensorflow_recommenders as tfrs
import pandas as pd

from models.layers import (
    StringEmbeddingLayer,
)


class BaselineModel(tfrs.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        items_dataset: tf.data.Dataset,
        embedding_size: int,
    ):
        """
        data: DataFrame of user-item interactions along with their corresponding features
        items_dataset: TF dataset containing the features for each item
        embedding_size: Final embedding size for the items & the users
        """
        super().__init__()

        # dict of preprocessing layers for each user feature

        self.user_model = StringEmbeddingLayer(
            embedding_size=embedding_size
        ).build_sequential_layer(vocabulary=data.user_id.unique())
        self.item_model = StringEmbeddingLayer(
            embedding_size=embedding_size
        ).build_sequential_layer(vocabulary=data.item_id.unique())

        self.task = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=items_dataset.map(self.item_model),
            ),
        )

    def compute_loss(self, inputs, training=False):
        # NB: maybe need to encapsulate inputs inside a list
        user_id, item_id = inputs
        user_embedding = self.user_model(user_id)
        item_embedding = self.item_model(item_id)

        return self.task(user_embedding, item_embedding, compute_metrics=not training)
