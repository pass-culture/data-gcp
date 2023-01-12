import tensorflow as tf
import pandas as pd

from tensorflow.keras.layers import Embedding, TextVectorization, Dot
from tensorflow.keras.layers.experimental.preprocessing import (
    StringLookup,
    IntegerLookup,
)

from models.v1.margin_loss import MarginLoss


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
        self.embedding_layers = []
        for layer_name, layer_info in self.layer_infos.items():
            self.embedding_layers.append(
                self.create_embedding_layer(
                    data=user_data,
                    column_name=layer_name,
                    lookup_type=layer_info["type"],
                    feature_latent_dim=layer_info["feature_latent_dim"],
                )
            )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size, activation="relu")

    def call(self, inputs: str, training=False):
        inputs = tf.unstack(tf.transpose(inputs))

        embeddings = []
        for idx, layer_info in enumerate(self.layer_infos.values()):
            if layer_info["type"] == "int":
                inputs[idx] = tf.strings.to_number(inputs[idx], tf.int32)
            embeddings.append(self.embedding_layers[idx](inputs[idx]))

        x = tf.concat(
            embeddings,
            axis=1,
        )
        x = self._dense1(x)
        out = self._dense2(x)
        return out

    @staticmethod
    def create_embedding_layer(
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
                        name=column_name,
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
                        name=column_name,
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
                        name=column_name,
                    ),
                    tf.keras.layers.GlobalAveragePooling1D(),
                ]
            )
        raise ValueError(
            "lookup_type passed in create_lookup_layer must be integer, text or string"
        )
