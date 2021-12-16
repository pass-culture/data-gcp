import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Dot
from tensorflow.keras.layers.experimental.preprocessing import StringLookup


class MFModel(Model):
    def __init__(self, user_ids, item_ids, user_vecs, item_vecs):
        super().__init__(name="MFModel")

        # self.margin = margin

        # l2_reg = None if l2_reg == 0 else l2(l2_reg)
        self.user_ids = user_ids
        self.item_ids = item_ids

        self.user_vecs = user_vecs
        self.item_vecs = item_vecs

        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    len(self.user_ids)
                    + 1,  # +1 was default, so we need additional one for the cs_user
                    20,
                    input_length=1,
                    input_shape=(1,),
                    name="user_embedding",
                    embeddings_initializer=tf.keras.initializers.Constant(
                        self.user_vecs
                    ),
                    trainable=False,
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
                    len(self.item_ids),
                    20,
                    input_length=1,
                    input_shape=(1,),
                    name="item_embedding",
                    embeddings_initializer=tf.keras.initializers.Constant(
                        self.item_vecs
                    ),
                    trainable=False,
                ),
            ]
        )

        # The 2 following layers are without parameters, and can
        # therefore be used for both positive and negative items.

        self.flatten = Flatten()
        self.dot = Dot(axes=1, normalize=False)

        # self.margin_loss = MarginLoss(margin)

    def call(self, inputs):
        user_input = inputs[0]
        # print("user_input: ",user_input)
        positive_item_input = inputs[1]
        # print("positive_item_input: ",user_input)

        user_embedding = self.user_layer(user_input)
        user_embedding = self.flatten(user_embedding)

        positive_item_embedding = self.item_layer(positive_item_input)
        positive_item_embedding = self.flatten(positive_item_embedding)

        # positive_similarity = tf.tensordot(user_embedding,positive_item_embedding,axes=2,name=None)

        positive_similarity = self.dot([user_embedding, positive_item_embedding])
        return positive_similarity

    def get_config(self):
        return {
            "user_ids": self.user_ids,
            "item_ids": self.item_ids,
            "user_vecs": self.user_vecs,
            "item_vecs": self.item_vecs,
        }

    @classmethod
    def from_config(cls, config):
        return cls(**config)
