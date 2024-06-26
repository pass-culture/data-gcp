import tensorflow as tf
from tensorflow.keras.layers import Dot, Embedding, Flatten, Layer
from tensorflow.keras.layers.experimental.preprocessing import StringLookup
from tensorflow.keras.models import Model
from tensorflow.keras.regularizers import l2


class MarginLoss(Layer):
    def __init__(self, margin=1.0):
        super().__init__()
        self.margin = margin

    def call(self, inputs):
        positive_pair_similarity = inputs[0]
        negative_pair_similarity = inputs[1]

        diff = negative_pair_similarity - positive_pair_similarity
        return tf.maximum(diff + self.margin, 0.0)


class TripletModel(Model):
    def __init__(self, user_ids, item_ids, latent_dim=64, l2_reg=None, margin=1.0):
        super().__init__(name="TripletModel")

        self.margin = margin

        l2_reg = None if l2_reg == 0 else l2(l2_reg)

        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    len(user_ids) + 1,
                    latent_dim,
                    input_length=1,
                    input_shape=(1,),
                    name="user_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        # The following embedding parameters will be shared to
        # encode both the positive and negative items.
        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    len(item_ids) + 1,
                    latent_dim,
                    input_length=1,
                    input_shape=(1,),
                    name="item_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        # The 2 following layers are without parameters, and can
        # therefore be used for both positive and negative items.
        self.flatten = Flatten()
        self.dot = Dot(axes=1, normalize=True)

        self.margin_loss = MarginLoss(margin)

    def call(self, inputs, training=False):
        user_input = inputs[0]
        positive_item_input = inputs[1]
        negative_item_input = inputs[2]

        user_embedding = self.user_layer(user_input)
        user_embedding = self.flatten(user_embedding)

        positive_item_embedding = self.item_layer(positive_item_input)
        positive_item_embedding = self.flatten(positive_item_embedding)

        negative_item_embedding = self.item_layer(negative_item_input)
        negative_item_embedding = self.flatten(negative_item_embedding)

        # Similarity computation between embeddings
        positive_similarity = self.dot([user_embedding, positive_item_embedding])
        negative_similarity = self.dot([user_embedding, negative_item_embedding])

        return self.margin_loss([positive_similarity, negative_similarity])
