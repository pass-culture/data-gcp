import tensorflow as tf
import pandas as pd

from tensorflow.keras.layers import Embedding, TextVectorization, Dot
from tensorflow.keras.layers.experimental.preprocessing import (
    StringLookup,
    IntegerLookup,
)

from models.v1.margin_loss import MarginLoss


def create_lookup_layer(
    data: pd.DataFrame,
    column_name: str,
    lookup_type: str,
    feature_latent_dim: int,
):
    vocabulary = data[column_name].unique()
    if lookup_type == "string":
        return tf.keras.Sequential(
            [
                StringLookup(vocabulary=vocabulary),
                Embedding(
                    input_dim=len(vocabulary) + 1,
                    output_dim=feature_latent_dim,
                ),
            ]
        )
    elif lookup_type == "int":
        return tf.keras.Sequential(
            [
                IntegerLookup(vocabulary=vocabulary),
                Embedding(
                    input_dim=len(vocabulary) + 1,
                    output_dim=feature_latent_dim,
                ),
            ]
        )
    elif lookup_type == "text":
        text_dataset = tf.data.Dataset.from_tensor_slices(vocabulary)
        max_tokens = 5000
        text_vectorization_layer = TextVectorization(
            max_tokens=max_tokens,
        )
        text_vectorization_layer.adapt(text_dataset.batch(64))
        return tf.keras.Sequential(
            [
                text_vectorization_layer,
                Embedding(
                    input_dim=max_tokens,
                    output_dim=feature_latent_dim,
                ),
            ]
        )
    raise ValueError(
        "lookup_type passed in create_lookup_layer must be integer, text or string"
    )


class TwoTowersModel(tf.keras.models.Model):
    def __init__(
        self,
        user_data: pd.DataFrame,
        user_layer_infos: dict,
        item_data: pd.DataFrame,
        item_layer_infos: dict,
        embedding_size: int,
        margin: int = 1,
    ):
        super().__init__("TwoTowerModel")

        self.user_model = SingleTowerModel(user_data, embedding_size, user_layer_infos)
        self.item_model = SingleTowerModel(item_data, embedding_size, item_layer_infos)

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


class SingleTowerModel(tf.keras.models.Model):
    def __init__(
        self,
        user_data: pd.DataFrame,
        embedding_size: int,
        layer_infos: dict,
    ):
        super().__init__(name="SingleTowerModel")

        self.user_data = user_data

        self.layer_infos = layer_infos
        self.layers = []
        for key, value in self.layer_infos.items():
            self.layers.append(
                create_lookup_layer(
                    data=user_data,
                    column_name=key,
                    lookup_type=value["type"],
                    feature_latent_dim=value["feature_latent_dim"],
                )
            )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size, activation="relu")

    def call(self, inputs: str):
        inputs = tf.unstack(tf.transpose(inputs))

        input_data = []
        for idx, value in enumerate(self.layer_infos.values()):
            if value["type"] == "int":
                inputs[idx] = tf.strings.to_number(inputs[idx], tf.int32)
            input_data.append(inputs[idx])

        x = tf.concat(
            [layer(input_layer_data) for layer, input_layer_data in zip(self.layers, input_data)],
            axis=1,
        )
        x = self._dense1(x)
        out = self._dense2(x)
        return out
