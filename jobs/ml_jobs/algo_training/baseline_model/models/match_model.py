import numpy as np
import tensorflow as tf
from tensorflow.keras.layers import Dot, Embedding
from tensorflow.keras.layers.experimental.preprocessing import StringLookup


class MatchModel(tf.keras.models.Model):
    def __init__(
        self,
        user_ids: list,
        item_ids: list,
        embedding_size: int,
    ):
        super().__init__()

        self.embedding_size = embedding_size

        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(user_ids) + 1,
                    input_shape=(1,),
                    input_length=1,
                    output_dim=self.embedding_size,
                    trainable=False,
                    name="user_embedding",
                ),
            ]
        )

        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(item_ids) + 1,
                    input_shape=(1,),
                    input_length=1,
                    output_dim=self.embedding_size,
                    trainable=False,
                    name="item_embedding",
                ),
            ]
        )

        self.dot = Dot(axes=1, normalize=True)
        self.flatten = tf.keras.layers.Flatten()

        self.initialize_weights()

    def call(self, inputs):
        user_id, item_id = inputs

        user_embedding = self.user_layer(user_id)
        user_embedding = self.flatten(user_embedding)

        item_embedding = self.item_layer(item_id)
        item_embedding = self.flatten(item_embedding)

        return self.dot([user_embedding, item_embedding])

    def initialize_weights(self):
        return self.predict([np.array(["fake_user_id"]), np.array(["fake_item_id"])])

    def set_embeddings(self, user_embeddings: np.ndarray, item_embeddings: np.ndarray):
        """
        Setting the weights of each Embedding layer (item & user) with the embeddings computed
        by the trained model
        """
        self.user_layer.layers[1].set_weights(
            [
                np.concatenate(
                    [
                        np.zeros((1, self.embedding_size)),
                        user_embeddings,
                    ],
                    axis=0,
                )
            ]
        )
        self.item_layer.layers[1].set_weights(
            [
                np.concatenate(
                    [
                        np.zeros((1, self.embedding_size)),
                        item_embeddings,
                    ],
                    axis=0,
                )
            ]
        )

    def get_config(self):
        return {"user_layer": self.user_layer, "item_layer": self.item_layer}

    @classmethod
    def from_config(cls, config):
        return cls(**config)
