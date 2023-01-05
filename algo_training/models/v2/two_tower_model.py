import tensorflow as tf
import pandas as pd

from tensorflow.keras.layers import Embedding, Flatten, Input, Dense, Lambda, Dot
from tensorflow.keras.layers.experimental.preprocessing import (
    StringLookup,
    IntegerLookup,
)

from models.v1.margin_loss import MarginLoss


class TwoTowerModel(tf.keras.models.Model):
    def __init__(
        self,
        user_data: pd.DataFrame,
        item_data: pd.DataFrame,
        embedding_size: int,
        margin: int = 1,
    ):
        super().__init__("TwoTowerModel")

        self.user_model = UserModel(user_data, embedding_size)
        self.user_layer = self.user_model.user_layer

        self.item_model = ItemModel(item_data, embedding_size)
        self.item_layer = self.item_model.item_layer

        self.dot = Dot(axes=1, normalize=True)
        self.margin_loss = MarginLoss(margin)

    def call(self, inputs, training=False):
        anchor_data, positive_data, negative_data = inputs

        user_embedding = self.user_model(anchor_data)
        positive_item_embedding = self.item_model(positive_data)
        negative_item_embedding = self.item_model(negative_data)

        positive_similarity = self.dot([user_embedding, positive_item_embedding])
        negative_similarity = self.dot([user_embedding, negative_item_embedding])

        return self.margin_loss([positive_similarity, negative_similarity])


class UserModel(tf.keras.models.Model):
    def __init__(
        self,
        user_data: pd.DataFrame,
        embedding_size: int,
        feature_latent_dim: int = 64,
    ):
        super().__init__(name="UserModel")

        self.user_data = user_data

        user_ids = self.user_data["user_id"].unique()
        self.user_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=user_ids),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(user_ids) + 1,
                    output_dim=feature_latent_dim,
                    name="user_embedding",
                ),
            ]
        )

        user_ages = self.user_data["user_age"].unique()
        self.user_age_layer = tf.keras.Sequential(
            [
                IntegerLookup(vocabulary=user_ages),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(user_ages) + 1,
                    output_dim=feature_latent_dim,
                    name="category_embedding",
                ),
            ]
        )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size, activation="relu")

    def call(self, inputs: str):
        inputs = tf.transpose(inputs)

        user_id = inputs[0]
        user_age = tf.strings.to_number(inputs[1], tf.int32)

        x = tf.concat(
            [
                self.user_layer(user_id),
                self.user_age_layer(user_age),
            ],
            axis=1,
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
    ):
        super().__init__(name="ItemModel")

        self.item_data = item_data

        item_ids = self.item_data["item_id"].unique()
        self.item_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=item_ids),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(item_ids) + 1,
                    output_dim=feature_latent_dim,
                    name="item_embedding",
                ),
            ]
        )

        categories = self.item_data["offer_categoryId"].unique()
        self.category_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=categories),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(categories) + 1,
                    output_dim=feature_latent_dim,
                    name="category_embedding",
                ),
            ]
        )

        subcategories = self.item_data["offer_subcategoryid"].unique()
        self.subcategory_layer = tf.keras.Sequential(
            [
                StringLookup(vocabulary=subcategories),
                # We add an additional embedding to account for unknown tokens.
                Embedding(
                    input_dim=len(subcategories) + 1,
                    output_dim=feature_latent_dim,
                    name="category_embedding",
                ),
            ]
        )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size, activation="relu")

    def call(self, inputs: str):
        inputs = tf.transpose(inputs)

        item_id, category_id, subcategory_id = inputs

        x = tf.concat(
            [
                self.item_layer(item_id),
                self.category_layer(category_id),
                self.subcategory_layer(subcategory_id),
            ],
            axis=1,
        )
        x = self._dense1(x)
        out = self._dense2(x)
        return out
