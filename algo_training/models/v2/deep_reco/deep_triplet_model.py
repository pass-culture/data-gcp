import tensorflow as tf
from tensorflow.keras import layers
from tensorflow.keras.models import Model
from tensorflow.keras.layers import (
    Embedding,
    Flatten,
    Dense,
    StringLookup,
    Concatenate,
    Dropout,
)
from tensorflow.keras.regularizers import l2


class MLP(layers.Layer):
    def __init__(self, n_hidden=1, hidden_size=64, dropout=0.0, l2_reg=None, **kwargs):
        super(MLP, self).__init__(**kwargs)

        self.n_hidden = n_hidden
        self.hidden_size = hidden_size
        self.dropout = dropout
        self.l2_reg = l2_reg

        self.layers = [Dropout(dropout)]

        for _ in range(n_hidden):
            self.layers.append(
                Dense(hidden_size, activation="relu", kernel_regularizer=l2_reg)
            )
            self.layers.append(Dropout(dropout))

        self.layers.append(Dense(1, activation="relu", kernel_regularizer=l2_reg))

    def call(self, x, training=False):
        for layer in self.layers:
            if isinstance(layer, Dropout):
                x = layer(x, training=training)
            else:
                x = layer(x)
        return x

    def get_config(self):
        config = super(MLP, self).get_config()
        config.update(
            {
                "n_hidden": self.n_hidden,
                "hidden_size": self.hidden_size,
                "dropout": self.dropout,
                "l2_reg": self.l2_reg,
            }
        )
        return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)


class DeepTripletModel(Model):
    def __init__(
        self,
        user_ids,
        item_ids,
        subcategories,
        user_dim=32,
        item_dim=64,
        subcategories_dim=32,
        margin=1.0,
        n_hidden=1,
        hidden_size=64,
        dropout=0,
        l2_reg=None,
    ):
        super().__init__()

        l2_reg = None if l2_reg == 0 else l2(l2_reg)

        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    len(user_ids) + 1,
                    user_dim,
                    input_length=1,
                    input_shape=(1,),
                    name="user_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    len(item_ids) + 1,
                    item_dim,
                    input_length=1,
                    input_shape=(1,),
                    name="item_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        self.subcategory_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=subcategories, mask_token=None),
                Embedding(
                    len(subcategories) + 1,
                    subcategories_dim,
                    input_length=1,
                    input_shape=(1,),
                    name="subcategories_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        self.flatten = Flatten()
        self.concat = Concatenate()

        self.mlp = MLP(n_hidden, hidden_size, dropout, l2_reg)

        self.margin_loss = MarginLoss(margin)

    def call(self, inputs, training=False):
        user_input = inputs[0]

        pos_item_id_input = inputs[1]
        pos_item_subcategory_input = inputs[2]

        neg_item_id_input = inputs[3]
        neg_item_subcategory_input = inputs[4]

        # Handling user
        user_embedding = self.user_layer(user_input)
        user_embedding = self.flatten(user_embedding)

        # Handling positive item
        pos_item_embedding = self.item_layer(pos_item_id_input)
        pos_item_embedding = self.flatten(pos_item_embedding)

        pos_item_subcategory = self.subcategory_layer(pos_item_subcategory_input)
        pos_item_subcategory = self.flatten(pos_item_subcategory)

        # Handling negative item
        neg_item_embedding = self.item_layer(neg_item_id_input)
        neg_item_embedding = self.flatten(neg_item_embedding)

        neg_item_subcategory = self.subcategory_layer(neg_item_subcategory_input)
        neg_item_subcategory = self.flatten(neg_item_subcategory)

        # Similarity computation between embeddings
        pos_tensor = self.concat(
            [user_embedding, pos_item_embedding, pos_item_subcategory]
        )
        neg_tensor = self.concat(
            [user_embedding, neg_item_embedding, neg_item_subcategory]
        )

        pos_similarity = self.mlp(pos_tensor)
        neg_similarity = self.mlp(neg_tensor)

        return self.margin_loss([pos_similarity, neg_similarity])
