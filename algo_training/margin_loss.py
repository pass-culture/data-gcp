from tensorflow.keras import layers


class MarginLoss(layers.Layer):
    def __init__(self, margin=1.0):
        super().__init__()
        self.margin = margin

    def call(self, inputs):
        pos_pair_similarity = inputs[0]
        neg_pair_similarity = inputs[1]

        diff = neg_pair_similarity - pos_pair_similarity
        return tf.maximum(diff + self.margin, 0.0)
