from collections import OrderedDict

import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs
from loguru import logger

from two_towers_model.utils.layers import (
    NumericalFeatureProcessor,
    PretainedEmbeddingLayer,
    StringEmbeddingLayer,
    TextEmbeddingLayer,
)

TEMPERATURE = 1.0


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
            tf.keras.losses.CategoricalCrossentropy(
                from_logits=True,
                reduction=tf.keras.losses.Reduction.SUM_OVER_BATCH_SIZE,
            ),
            metrics=self.get_metrics(item_dataset) if item_dataset else None,
            temperature=TEMPERATURE,
        )

    def get_metrics(self, item_dataset):
        index_top_k = tfrs.layers.factorized_top_k.BruteForce()

        item_embeddings = tf.concat(list(item_dataset.map(self.item_model)), axis=0)
        item_ids = tf.concat(
            list(item_dataset.map(lambda item: item[self._item_idx])), axis=0
        )

        index_top_k.index(candidates=item_embeddings, identifiers=item_ids)

        return tfrs.metrics.FactorizedTopK(candidates=index_top_k, ks=[10, 50])

    @staticmethod
    def load_embedding_layer(
        layer_type: str,
        embedding_size: int,
        embedding_initialisation_weights: pd.DataFrame = None,
    ):
        embedding_layers = {
            "string": StringEmbeddingLayer(embedding_size=embedding_size),
            "int": NumericalFeatureProcessor(output_size=embedding_size),
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
        user_features = {name: features[name] for name in self._user_feature_names}
        item_features = {name: features[name] for name in self._item_feature_names}
        candidate_sampling_probability = features["candidate_sampling_probability"]

        user_embeddings = self.user_model(user_features, training=training)
        item_embeddings = self.item_model(item_features, training=training)

        return self.task(
            query_embeddings=user_embeddings,
            candidate_embeddings=item_embeddings,
            compute_metrics=not training,
            compute_batch_metrics=not training,
            candidate_sampling_probability=candidate_sampling_probability,
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
            if isinstance(layer_class, NumericalFeatureProcessor):
                training_data = self.data[layer_name]
            else:
                training_data = self.data[layer_name].unique()
            self._embedding_layers[layer_name] = layer_class.build_sequential_layer(
                vocabulary=training_data
            )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size)

    def call(self, features: dict, training=False):
        feature_embeddings = []

        feature_embeddings = []
        for feature_name, embedding_layer in self._embedding_layers.items():
            processed = embedding_layer(features[feature_name])
            logger.debug(f"Processed {feature_name} embedding shape: {processed.shape}")
            feature_embeddings.append(processed)

        x = tf.concat(feature_embeddings, axis=1)
        x = self._dense1(x)
        return self._dense2(x)
