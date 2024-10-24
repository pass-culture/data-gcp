from collections import OrderedDict

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
        user_features_config: OrderedDict,
        item_features_config: OrderedDict,
        user_columns: list,
        item_columns: list,
        embedding_size: int,
    ):
        super().__init__()
        self._user_feature_names = user_columns
        self._item_feature_names = item_columns
        self._item_idx = self._item_feature_names.index("item_id")
        self._user_idx = self._user_feature_names.index("user_id")

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

    def set_task(self, item_dataset=None):
        self.task = tfrs.tasks.Retrieval(
            loss=tf.keras.losses.CategoricalCrossentropy(
                from_logits=True,
                reduction=tf.keras.losses.Reduction.SUM_OVER_BATCH_SIZE,
            ),
            metrics=self.get_metrics(item_dataset) if item_dataset else None,
        )

    def get_metrics(self, item_dataset):
        index_top_k = tfrs.layers.factorized_top_k.BruteForce()

        item_embeddings = tf.math.l2_normalize(
            item_dataset.map(self.item_model), axis=-1
        )
        item_ids = item_dataset.map(lambda item: item[self._item_idx])

        index_top_k.index(item_ids, item_embeddings)

        return tfrs.metrics.FactorizedTopK(candidates=index_top_k, ks=[10, 50])

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

    def compute_loss(self, features, training=False):
        user_feature_count = len(self._user_feature_names)
        item_feature_count = len(self._item_feature_names)

        user_features = features[:user_feature_count]
        item_features = features[
            user_feature_count : user_feature_count + item_feature_count
        ]
        user_embeddings = tf.math.l2_normalize(
            self.user_model(user_features, training=training), axis=-1
        )
        item_embeddings = tf.math.l2_normalize(
            self.user_model(item_features, training=training), axis=-1
        )

        return self.task(
            query_embeddings=user_embeddings,
            candidate_embeddings=item_embeddings,
            compute_metrics=not training,
            compute_batch_metrics=not training,
        )


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

        self._dropout1 = tf.keras.layers.Dropout(0.2)
        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dropout2 = tf.keras.layers.Dropout(0.1)
        self._dense2 = tf.keras.layers.Dense(embedding_size)

    def call(self, features: list, training=False):
        feature_embeddings = []
        for idx, embedding_layer in enumerate(self._embedding_layers.values()):
            feature_embeddings.append(embedding_layer(features[idx]))

        x = tf.concat(feature_embeddings, axis=1)

        x = self._dropout1(x, training=training)
        x = self._dense1(x)
        x = self._dropout2(x, training=training)
        x = self._dense2(x)
        return x
