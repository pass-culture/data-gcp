from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Input, Dense, Lambda, Dot
import tensorflow as tf


class MatchModel(Model):
    def __init__(self, user_layer, item_layer):
        super().__init__(name="MatchModel")

        self.user_layer = user_layer
        self.item_layer = item_layer

        self.flatten = Flatten()
        self.dot = Dot(axes=1, normalize=True)

    def call(self, inputs):
        nb_offers = tf.size(inputs[1:])
        user_input = tf.reshape([inputs[0]], (nb_offers, 1))
        user_input = tf.cast(user_input, dtype=tf.string)
        positive_item_input = tf.reshape(inputs[1:], (nb_offers, 1))

        user_embedding = self.user_layer(user_input)
        user_embedding = self.flatten(user_embedding)

        positive_item_embedding = self.item_layer(positive_item_input)
        positive_item_embedding = self.flatten(positive_item_embedding)

        positive_similarity = self.dot([user_embedding, positive_item_embedding])

        return positive_similarity

    def get_config(self):
        return {"user_layer": self.user_layer, "item_layer": self.item_layer}

    @classmethod
    def from_config(cls, config):
        return cls(**config)
