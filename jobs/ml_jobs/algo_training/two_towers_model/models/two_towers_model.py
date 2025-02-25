from collections import OrderedDict
from typing import Any, Callable, Dict, List, Union

import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs

from two_towers_model.utils.layers import (
    NumericalFeatureProcessor,
    PretainedEmbeddingLayer,
    StringEmbeddingLayer,
    TextEmbeddingLayer,
)

TEMPERATURE = 0.05


class TwoTowersModel(tfrs.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        user_features_config: OrderedDict,
        item_features_config: OrderedDict,
        user_columns: List[str],
        item_columns: List[str],
        user_tower_params: Dict[str, Any],
        item_tower_params: Dict[str, Any],
        remove_accidental_hits: bool = True,
    ):
        super().__init__()

        # Keep references
        self._user_feature_names = user_columns
        self._item_feature_names = item_columns
        self.remove_accidental_hits = remove_accidental_hits

        # Build user embedding layers from user_features_config
        user_embedding_layers = {}
        for name, layer in user_features_config.items():
            # e.g. layer["type"] might be "text", "string", "int", or "pretrained"
            # optionally you might read layer["embedding_size"]
            user_embedding_layers[name] = self.load_embedding_layer(
                layer_type=layer["type"],
                embedding_size=layer.get(
                    "embedding_size", user_tower_params.get("base_embedding_size", 64)
                ),
            )

        # Build item embedding layers from item_features_config
        item_embedding_layers = {}
        for name, layer in item_features_config.items():
            if layer["type"] == "pretrained":
                init_weights = data[name].drop_duplicates().tolist()
            else:
                init_weights = None

            item_embedding_layers[name] = self.load_embedding_layer(
                layer_type=layer["type"],
                embedding_size=layer.get(
                    "embedding_size", item_tower_params.get("base_embedding_size", 64)
                ),
                embedding_initialisation_weights=init_weights,
            )

        # Create user tower (SingleTowerModel) with user_tower_params
        self.user_model = SingleTowerModel(
            data=data,
            input_embedding_layers=user_embedding_layers,
            **user_tower_params,  # e.g. base_embedding_size=128, hidden_layer_dims=[256,128], etc.
        )

        # Create item tower (SingleTowerModel) with item_tower_params
        self.item_model = SingleTowerModel(
            data=data, input_embedding_layers=item_embedding_layers, **item_tower_params
        )

        # You’ll typically set up the retrieval task externally or inside a method:
        self.task = None  # we will define via set_task(...) below

    @staticmethod
    def load_embedding_layer(
        layer_type: str, embedding_size: int, embedding_initialisation_weights=None
    ):
        """Returns an appropriate embedding layer based on the type."""
        if layer_type == "text":
            return TextEmbeddingLayer(embedding_size=embedding_size)
        elif layer_type == "string":
            return StringEmbeddingLayer(embedding_size=embedding_size)
        elif layer_type == "int":
            return NumericalFeatureProcessor(output_size=embedding_size)
        elif layer_type == "pretrained":
            return PretainedEmbeddingLayer(
                embedding_size=embedding_size,
                embedding_initialisation_weights=embedding_initialisation_weights,
            )
        else:
            raise ValueError(f"Unknown layer_type: {layer_type}")

    def set_task(self, item_dataset):
        """Create a TFRS retrieval task with optional negative sampling + metrics."""
        self.task = tfrs.tasks.Retrieval(
            loss=tfrs.losses.SampledSoftmaxLoss(
                remove_accidental_hits=self.remove_accidental_hits
            ),
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=item_dataset.map(self.item_model)  # or a BruteForce index
            ),
        )

    def compute_loss(self, features, training=False):
        """Called by TFRS at training time."""
        # Extract user & item features from the input
        user_features = {name: features[name] for name in self._user_feature_names}
        item_features = {name: features[name] for name in self._item_feature_names}

        # Compute embeddings
        user_embeddings = self.user_model(user_features, training=training)
        item_embeddings = self.item_model(item_features, training=training)

        # Pass to the TFRS retrieval task
        return self.task(
            query_embeddings=user_embeddings,
            candidate_embeddings=item_embeddings,
            compute_metrics=not training,
        )


class SingleTowerModel(tf.keras.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        input_embedding_layers: Dict[str, Any],
        base_embedding_size: int,
        hidden_layer_dims: List[int] = None,  # e.g., [128, 64]
        hidden_activation: Union[str, Callable] = "relu",
        dropout_rate: float = 0.0,
        use_batch_norm: bool = False,
        use_unit_norm: bool = True,
    ):
        super().__init__()

        self.data = data
        self.input_embedding_layers = input_embedding_layers
        self.use_unit_norm = use_unit_norm

        if hidden_layer_dims is None:
            # Default to a simple MLP if not specified
            hidden_layer_dims = [base_embedding_size * 2, base_embedding_size]

        # Build the embedding sub-layers
        self._embedding_layers = {}
        for layer_name, layer_class in self.input_embedding_layers.items():
            if isinstance(layer_class, NumericalFeatureProcessor):
                # For numerical features, pass the raw data
                training_data = self.data[layer_name]
            else:
                # For categorical/text features, pass unique tokens
                training_data = self.data[layer_name].unique()

            self._embedding_layers[layer_name] = layer_class.build_sequential_layer(
                vocabulary=training_data
            )

        # Build the dense stack (MLP)
        self._mlp_layers = []
        for dim in hidden_layer_dims:
            self._mlp_layers.append(
                tf.keras.layers.Dense(dim, activation=hidden_activation)
            )
            if use_batch_norm:
                self._mlp_layers.append(tf.keras.layers.BatchNormalization())
            if dropout_rate > 0.0:
                self._mlp_layers.append(tf.keras.layers.Dropout(dropout_rate))

        # Final normalization
        if use_unit_norm:
            self._norm = tf.keras.layers.UnitNormalization(axis=-1, name="l2_normalize")
        else:
            self._norm = None

    def call(self, features: Dict[str, tf.Tensor], training=False):
        # 1) Build embeddings from each feature
        feature_embeddings = []
        for feature_name, embedding_layer in self._embedding_layers.items():
            processed = embedding_layer(features[feature_name], training=training)
            feature_embeddings.append(processed)

        # 2) Concatenate all embeddings
        x = tf.concat(feature_embeddings, axis=1)

        # 3) Pass through the MLP (the dense stack)
        for layer in self._mlp_layers:
            x = layer(x, training=training)

        # 4) Optional final normalization
        if self._norm is not None:
            x = self._norm(x)

        return x
