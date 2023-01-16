import tensorflow as tf
import tensorflow_recommenders as tfrs
import pandas as pd


class TwoTowersModel(tfrs.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        user_embedding_layers: dict,
        item_embedding_layers: dict,
        items_dataset: tf.data.Dataset,
        embedding_size: int,
    ):
        """
        data: DataFrame of user-item interactions along with their corresponding features
        user_embedding_layers: dict of preprocessing layers for each user feature
        item_embedding_layers: dict of preprocessing layers for each item feature
        """
        super().__init__("TwoTowerModel")

        self._user_feature_names = user_embedding_layers.keys()
        self._item_feature_names = item_embedding_layers.keys()

        self.user_model = SingleTowerModel(
            data=data, layer_infos=user_embedding_layers, embedding_size=embedding_size
        )
        self.item_model = SingleTowerModel(
            data=data, layer_infos=item_embedding_layers, embedding_size=embedding_size
        )

        self.task = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidates=items_dataset.map(self.item_model),
            ),
        )

    def compute_loss(self, features, training=False):
        user_embedding = self.user_model(
            {name: features[name] for name in self._user_feature_names}
        )
        item_embedding = self.item_model(
            {name: features[name] for name in self._item_feature_names}
        )

        return self.task(user_embedding, item_embedding, compute_metrics=not training)


class SingleTowerModel(tf.keras.models.Model):
    def __init__(
        self,
        data: pd.DataFrame,
        layer_infos: dict,
        embedding_size: int,
    ):
        super().__init__()

        self.data = data
        self.layer_infos = layer_infos

        self._embedding_layers = {}
        for layer_name, layer_class in self.layer_infos.items():
            self._embedding_layers[layer_name] = layer_class.build_sequential_layer(
                vocabulary=self.data[layer_name].unique()
            )

        self._dense1 = tf.keras.layers.Dense(embedding_size * 2, activation="relu")
        self._dense2 = tf.keras.layers.Dense(embedding_size)

    def call(self, features: str, training=False):
        feature_embeddings = []
        for feature_name, feature_data in features.items():
            feature_embeddings.append(
                self._embedding_layers[feature_name](feature_data)
            )

        x = tf.concat(
            feature_embeddings,
            axis=1,
        )
        x = self._dense1(x)
        out = self._dense2(x)
        return out
