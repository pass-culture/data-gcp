import tensorflow as tf
import pandas as pd

from tensorflow.keras.layers import Embedding, Flatten, Input, Dense, Lambda, Dot
from tensorflow.keras.regularizers import l2
from tensorflow.keras.layers.experimental.preprocessing import (
    StringLookup,
    IntegerLookup,
)


class TwoTowerModel(tf.keras.models.Model):
    def __init__(self, user_data: pd.DataFrame, item_data: pd.DataFrame, embedding_size: int, l2_reg: int = None):
        super().__init__("TwoTowerModel")

        self.user_model = UserModel(user_data, embedding_size, l2_reg)
        self.item_model = ItemModel(item_data, embedding_size, l2_reg)

    def call(self, inputs, training=False):
        # At training output the margin loss between positive & negative examples
        if training:
            anchor_data, positive_data, negative_data = inputs

            user_embedding = self.user_model(anchor_data)
            positive_item_embedding = self.item_model(positive_data)
            negative_item_embedding = self.item_model(negative_data)

            positive_similarity = self.dot([user_embedding, positive_item_embedding])
            negative_similarity = self.dot([user_embedding, negative_item_embedding])

            return self.margin_loss([positive_similarity, negative_similarity])

        # At inference, output the distance between user & item
        user_id, item_id = inputs

        user_embedding = self.user_model(user_id)
        item_embedding = self.item_model(item_id)

        return self.dot([user_embedding, item_embedding])


class UserModel(tf.keras.models.Model):
    def __init__(
        self,
        user_data: pd.DataFrame,
        embedding_size: int,
        feature_latent_dim: int = 64,
        l2_reg: int = None,
    ):
        super().__init__(name="UserModel")

        self.user_data = user_data

        l2_reg = None if l2_reg == 0 else l2(l2_reg)

        user_ids = self.user_data["item_id"].unique()
        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(user_ids) + 1,
                    output_dim=feature_latent_dim,
                    name="item_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        user_ages = self.user_data["user_age"].unique()
        self.user_age_layer = tf.keras.Sequential(
            [
                IntegerLookup(vocabulary=user_ages, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(user_ages) + 1,
                    output_dim=feature_latent_dim,
                    name="category_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )
        self._dense1 = tf.keras.layers.Dense(
            embedding_size * 2, activation="relu", kernel_regularizer=l2_reg
        )
        self._dense2 = tf.keras.layers.Dense(
            embedding_size, activation="relu", kernel_regularizer=l2_reg
        )

    def call(self, user_id: str):
        user_age = self.user_data[lambda df: df["user_id"] == user_id]["user_age"].values[0]

        x = tf.concat(
            [
                self.user_layer(user_id),
                self.user_age_layer(user_age),
            ], axis=1
        )
        x = self._dense1(x)
        out = self._dense2(x)
        return out


class ItemModel(tf.keras.models.Model):
    def __init__(
        self,
        item_data: pd.DataFrame,
        embedding_size: int,
        feature_latent_dim: int = 64,
        l2_reg: int = None,
    ):
        super().__init__(name="ItemModel")

        self.item_data = item_data

        l2_reg = None if l2_reg == 0 else l2(l2_reg)

        item_ids = self.item_data["item_id"].unique()
        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(item_ids) + 1,
                    output_dim=feature_latent_dim,
                    name="item_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )

        categories = self.item_data["offer_categoryId"].unique()
        self.category_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=categories, mask_token=None),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(categories) + 1,
                    output_dim=feature_latent_dim,
                    name="category_embedding",
                    embeddings_regularizer=l2_reg,
                ),
            ]
        )
        self._dense1 = tf.keras.layers.Dense(
            embedding_size * 2, activation="relu", kernel_regularizer=l2_reg
        )
        self._dense2 = tf.keras.layers.Dense(
            embedding_size, activation="relu", kernel_regularizer=l2_reg
        )

    def call(self, item_id: str):
        category_id = self.item_data[lambda df: df["item_id"] == item_id][
            "offer_categoryId"
        ].values[0]
        x = tf.concat(
            [
                self.item_layer(item_id),
                self.category_layer(category_id),
            ], axis=1
        )
        x = self._dense1(x)
        out = self._dense2(x)
        return out
