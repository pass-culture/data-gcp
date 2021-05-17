from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Input, Dense
from tensorflow.keras.layers import Lambda, Dot
from tensorflow.keras.regularizers import l2


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
        pos_item_input = inputs[1]
        neg_item_input = inputs[2]

        user_embedding = self.user_layer(user_input)
        user_embedding = self.flatten(user_embedding)

        pos_item_embedding = self.item_layer(pos_item_input)
        pos_item_embedding = self.flatten(pos_item_embedding)

        neg_item_embedding = self.item_layer(neg_item_input)
        neg_item_embedding = self.flatten(neg_item_embedding)

        # Similarity computation between embeddings
        pos_similarity = self.dot([user_embedding, pos_item_embedding])
        neg_similarity = self.dot([user_embedding, neg_item_embedding])

        return self.margin_loss([pos_similarity, neg_similarity])
