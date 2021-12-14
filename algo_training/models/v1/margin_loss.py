import tensorflow as tf
from tensorflow.keras import layers


class MarginLoss(layers.Layer):
    def __init__(self, margin=1.0):
        super().__init__()
        self.margin = margin

    def call(self, inputs):
        positive_pair_similarity = inputs[0]
        negative_pair_similarity = inputs[1]

        diff = negative_pair_similarity - positive_pair_similarity
        return tf.maximum(diff + self.margin, 0.0)
