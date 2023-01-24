import tensorflow as tf
import numpy as np

from tensorflow.keras.layers import Embedding, Dot
from tensorflow.keras.layers.experimental.preprocessing import StringLookup


class TwoTowersMatchModel(tf.keras.models.Model):
    def __init__(
        self,
        user_ids: list,
        item_ids: list,
        embedding_size: int,
    ):
        super().__init__()

        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids),
                # We add an additional embedding to account for [UNK] and '' tokens
                Embedding(
                    input_dim=len(user_ids) + 2,
                    input_shape=(1,),
                    input_length=1,
                    output_dim=embedding_size,
                    trainable=False,
                    name="user_embedding",
                ),
            ]
        )

        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids),
                # We add an additional embedding to account for [UNK] and '' tokens
                Embedding(
                    input_dim=len(item_ids) + 2,
                    input_shape=(1,),
                    input_length=1,
                    output_dim=embedding_size,
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

    def get_config(self):
        return {"user_layer": self.user_layer, "item_layer": self.item_layer}

    @classmethod
    def from_config(cls, config):
        return cls(**config)
