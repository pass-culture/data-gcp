import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs
from loguru import logger

from two_towers_model.utils.layers import (
    IntegerEmbeddingLayer,
    PretainedEmbeddingLayer,
    SequenceEmbeddingLayer,
    StringEmbeddingLayer,
    TextEmbeddingLayer,
    TimestampEmbeddingLayer,
)


class TwoTowersModel(tfrs.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        user_features_config: dict,
        item_features_config: dict,
        items_dataset: tf.data.Dataset,
        embedding_size: int,
    ):
        """
        data: DataFrame of user-item interactions along with their corresponding features
        user_features_config: dict containing the information about the user input embedding layers
        item_features_config: dict containing the information about the item input embedding layers
        items_dataset: TF dataset containing the features for each item
        embedding_size: Final embedding size for the items & the users
        """
        super().__init__()

        max_timestamp = data["timestamp"].max()
        min_timestamp = data["timestamp"].min()
        timestamp_buckets = np.linspace(min_timestamp, max_timestamp, num=1000)
        # dict of preprocessing layers for each user feature
        # Decide if we put the timestamp bucket in user or item layer
        user_embedding_layers = {
            name: self.load_embedding_layer(
                layer_type=layer["type"],
                embedding_size=layer["embedding_size"],
            )
            for name, layer in user_features_config.items()
        }
        self._user_feature_names = user_features_config.keys()

        # dict of preprocessing layers for each item feature
        item_embedding_layers = {
            name: self.load_embedding_layer(
                layer_type=layer["type"],
                embedding_size=layer["embedding_size"],
                embedding_initialisation_weights=data[name].drop_duplicates().tolist()
                if layer["type"] == "pretrained"
                else None,
            )
            for name, layer in item_features_config.items()
        }

        self._item_feature_names = item_features_config.keys()

        logger.info("Building user & item models")
        logger.info(f"User features: {self._user_feature_names}")
        self.user_model = SingleTowerModel(
            data=data,
            input_embedding_layers=user_embedding_layers,
            embedding_size=embedding_size,
            history_sequence=True,
            timestamp_buckets=timestamp_buckets,
        )
        logger.info(f"Item features: {self._item_feature_names} ")
        self.item_model = SingleTowerModel(
            data=data,
            input_embedding_layers=item_embedding_layers,
            embedding_size=embedding_size,
        )

        self.task = tfrs.tasks.Retrieval(
            loss=tf.keras.losses.CategoricalCrossentropy(
                from_logits=True, reduction=tf.keras.losses.Reduction.SUM
            ),
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=tfrs.layers.factorized_top_k.BruteForce().index_from_dataset(
                    items_dataset.map(self.item_model)
                ),
                ks=[50],
            ),
        )

    def compute_loss(self, features, training=False):
        user_features = {name: features[name] for name in self._user_feature_names}
        user_features["previous_item_id"] = features["previous_item_id"]
        user_features["timestamp"] = features["timestamp"]

        item_features = {name: features[name] for name in self._item_feature_names}

        user_embedding = tf.math.l2_normalize(self.user_model(user_features), axis=1)
        item_embedding = tf.math.l2_normalize(self.item_model(item_features), axis=1)
        # TODO add implicit_weights = features["click"]
        # sample_weight=implicit_weights
        return self.task(user_embedding, item_embedding, compute_metrics=not training)

    @staticmethod
    def load_embedding_layer(
        layer_type: str,
        embedding_size: int,
        embedding_initialisation_weights: pd.DataFrame = None,
    ):
        try:
            return {
                "string": StringEmbeddingLayer(embedding_size=embedding_size),
                "int": IntegerEmbeddingLayer(embedding_size=embedding_size),
                "text": TextEmbeddingLayer(embedding_size=embedding_size),
                "pretrained": PretainedEmbeddingLayer(
                    embedding_size=embedding_size,
                    embedding_initialisation_weights=embedding_initialisation_weights,
                ),
                "sequence": SequenceEmbeddingLayer(embedding_size=embedding_size),
                # "timestamp": TimestampEmbeddingLayer(embedding_size=embedding_size),
            }[layer_type]
        except KeyError:
            raise ValueError(
                f"InvalidLayerType: The features config file contains an invalid layer type `{layer_type}`"
            )


class SingleTowerModel(tf.keras.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        input_embedding_layers: dict,
        embedding_size: int,
        timestamp_buckets: np.ndarray = None,
        history_sequence: bool = False,
    ):
        super().__init__()

        self.data = data
        self.input_embedding_layers = input_embedding_layers

        self._embedding_layers = {}
        for layer_name, layer_class in self.input_embedding_layers.items():
            logger.info(f"Building layer {layer_name}")
            self._embedding_layers[layer_name] = layer_class.build_sequential_layer(
                vocabulary=self.data[layer_name].unique()
            )
        if timestamp_buckets is not None:
            self._embedding_layers["timestamp"] = TimestampEmbeddingLayer(
                embedding_size=embedding_size
            ).build_sequential_layer(timestamp_buckets=timestamp_buckets)
        if history_sequence:
            self._embedding_layers["previous_item_id"] = SequenceEmbeddingLayer(
                embedding_size=embedding_size
            ).build_sequential_layer(vocabulary=self.data["item_id"].unique())
        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size)

    def call(self, features: dict, training=False):
        feature_embeddings = []

        for layer_name, embedding_layer in self._embedding_layers.items():
            # Ensure previous_item_id is in the correct 3D shape
            # if layer_name == "previous_item_id":
            #     features["previous_item_id"] = tf.expand_dims(
            #         features["previous_item_id"], axis=1
            #     )
            feature_embeddings.append(embedding_layer(features[layer_name]))

        x = tf.concat(
            feature_embeddings,
            axis=1,
        )

        x = self._dense1(x)
        x = self._dense2(x)
        return x
