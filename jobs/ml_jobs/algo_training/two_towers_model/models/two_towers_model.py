import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs

from two_towers_model.utils.layers import (
    IntegerEmbeddingLayer,
    PretainedEmbeddingLayer,
    StringEmbeddingLayer,
    TextEmbeddingLayer,
)


class TwoTowersModel(tfrs.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        user_features_config: dict,
        item_features_config: dict,
        item_dataset: tf.data.Dataset,
        embedding_size: int,
    ):
        super().__init__()
        self._item_feature_names = item_features_config.keys()
        self._user_feature_names = user_features_config.keys()

        # Define user and item models
        self.user_model = SingleTowerModel(
            data=data,
            input_embedding_layers={
                name: self.load_embedding_layer(
                    layer_type=layer["type"],
                    embedding_size=layer["embedding_size"],
                )
                for name, layer in user_features_config.items()
            },
            embedding_size=embedding_size,
        )

        self.item_model = SingleTowerModel(
            data=data,
            input_embedding_layers={
                name: self.load_embedding_layer(
                    layer_type=layer["type"],
                    embedding_size=layer["embedding_size"],
                    embedding_initialisation_weights=data[name]
                    .drop_duplicates()
                    .tolist()
                    if layer["type"] == "pretrained"
                    else None,
                )
                for name, layer in item_features_config.items()
            },
            embedding_size=embedding_size,
        )

        item_embeddings = item_dataset.map(self.item_model)
        index_top_k = tfrs.layers.factorized_top_k.BruteForce()
        index_top_k.index_from_dataset(item_embeddings)

        self.task = tfrs.tasks.Retrieval(
            loss=tf.keras.losses.CategoricalCrossentropy(
                from_logits=True, reduction=tf.keras.losses.Reduction.SUM
            ),
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=index_top_k,
                ks=[50],
            ),
        )

    def compute_loss(self, features, training=False):
        user_features, item_features = (
            features[: len(self._user_feature_names)],
            features[len(self._user_feature_names) :],
        )

        user_embedding = tf.math.l2_normalize(self.user_model(user_features), axis=1)
        item_embedding = tf.math.l2_normalize(self.item_model(item_features), axis=1)

        return self.task(
            user_embedding,
            item_embedding,
            compute_metrics=not training,
        )

    @staticmethod
    def load_embedding_layer(
        layer_type: str,
        embedding_size: int,
        embedding_initialisation_weights: pd.DataFrame = None,
    ):
        embedding_layers = {
            "string": StringEmbeddingLayer(embedding_size=embedding_size),
            "int": IntegerEmbeddingLayer(embedding_size=embedding_size),
            "text": TextEmbeddingLayer(embedding_size=embedding_size),
            "pretrained": PretainedEmbeddingLayer(
                embedding_size=embedding_size,
                embedding_initialisation_weights=embedding_initialisation_weights,
            ),
        }
        if layer_type not in embedding_layers:
            raise ValueError(
                f"InvalidLayerType: The features config file contains an invalid layer type `{layer_type}`"
            )
        return embedding_layers[layer_type]


class SingleTowerModel(tf.keras.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        input_embedding_layers: dict,
        embedding_size: int,
    ):
        super().__init__()

        self.data = data
        self.input_embedding_layers = input_embedding_layers

        self._embedding_layers = {}
        for layer_name, layer_class in self.input_embedding_layers.items():
            self._embedding_layers[layer_name] = layer_class.build_sequential_layer(
                vocabulary=self.data[layer_name].unique()
            )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._norm1 = tf.keras.layers.LayerNormalization(axis=-1)
        self._dense2 = tf.keras.layers.Dense(embedding_size)
        self._norm2 = tf.keras.layers.LayerNormalization(axis=-1)

    def call(self, features: dict, training=False):
        feature_embeddings = []
        for idx, embedding_layer in enumerate(self._embedding_layers.values()):
            feature_embeddings.append(embedding_layer(features[idx]))

        x = tf.concat(feature_embeddings, axis=1)

        x = self._dense1(x)
        x = self._norm1(x)
        x = self._dense2(x)
        x = self._norm2(x)
        return x
