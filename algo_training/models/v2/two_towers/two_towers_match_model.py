import tensorflow as tf
import numpy as np

from tensorflow.keras.layers import Embedding, Flatten, Input, Dense, Lambda, Dot
from tensorflow.keras.layers.experimental.preprocessing import StringLookup


class TwoTowersMatchModel(tf.keras.models.Model):
    def __init__(
        self,
        user_ids: list,
        user_embeddings: np.ndarray,
        item_ids: list,
        item_embeddings: np.ndarray,
        embedding_size: int,
    ):
        super().__init__(name="MatchModelV2")

        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids),
                Embedding(
                    input_dim=len(user_ids) + 1,
                    input_shape=(1,),
                    input_length=1,
                    output_dim=embedding_size,
                    embeddings_initializer=tf.keras.initializers.Constant(
                        np.concatenate(
                            [
                                np.zeros((1, embedding_size)),
                                user_embeddings,
                            ],
                            axis=0,
                        )
                    ),
                    trainable=False,
                    name="user_embedding",
                ),
            ]
        )

        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids),
                Embedding(
                    input_dim=len(item_ids) + 1,
                    input_shape=(1,),
                    input_length=1,
                    output_dim=embedding_size,
                    embeddings_initializer=tf.keras.initializers.Constant(
                        np.concatenate(
                            [
                                np.zeros((1, embedding_size)),
                                item_embeddings,
                            ],
                            axis=0,
                        )
                    ),
                    trainable=False,
                    name="item_embedding",
                ),
            ]
        )

        self.dot = Dot(axes=1, normalize=True)

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

    def get_config(self):
        return {"user_layer": self.user_layer, "item_layer": self.item_layer}

    @classmethod
    def from_config(cls, config):
        return cls(**config)
