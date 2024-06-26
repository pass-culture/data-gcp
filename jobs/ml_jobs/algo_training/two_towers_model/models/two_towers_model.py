import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs
from utils.layers import (
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

        # dict of preprocessing layers for each user feature
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

        self.user_model = SingleTowerModel(
            data=data,
            input_embedding_layers=user_embedding_layers,
            embedding_size=embedding_size,
        )
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
                # candidates=items_dataset.map(self.item_model),
                candidates=tfrs.layers.factorized_top_k.BruteForce().index_from_dataset(
                    items_dataset.map(self.item_model)
                ),
                ks=[50],
            ),
        )

    def compute_loss(self, features, training=False):
        user_features, item_features = (
            features[: len(self._user_feature_names)],
            features[len(self._user_feature_names) :],
        )

        user_embedding = self.user_model(user_features)
        item_embedding = self.item_model(item_features)
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
        self._dense2 = tf.keras.layers.Dense(embedding_size)

    def call(self, features: dict, training=False):
        feature_embeddings = []
        for idx, embedding_layer in enumerate(self._embedding_layers.values()):
            feature_embeddings.append(embedding_layer(features[idx]))

        x = tf.concat(
            feature_embeddings,
            axis=1,
        )

        x = self._dense1(x)
        x = self._dense2(x)
        return x
