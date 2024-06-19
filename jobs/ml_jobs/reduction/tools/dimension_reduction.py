import numpy as np
import io
from google.cloud import bigquery
import umap
from sklearn.decomposition import PCA
import tensorflow as tf


def get_sample(data, frac):
    sample_size = int(frac * len(data))
    return data[np.random.choice(len(data), size=sample_size, replace=False)]


def umap_reduce_embedding_dimension(
    data,
    dimension,
):
    return umap.UMAP(
        n_neighbors=10,
        n_components=dimension,
        init="random",
        metric="cosine",
        low_memory=True,
        unique=True,
    ).fit_transform(data)


def pumap_reduce_embedding_dimension(data, dimension, train_frac=0.1, batch_size=2048):
    keras_fit_kwargs = {
        "callbacks": [
            tf.keras.callbacks.EarlyStopping(
                monitor="loss",
                min_delta=10**-2,
                patience=2,
                verbose=1,
            )
        ],
        "verbose": 2,
    }

    embedder = umap.ParametricUMAP(
        n_components=dimension,
        n_neighbors=10,
        verbose=True,
        metric="cosine",
        keras_fit_kwargs=keras_fit_kwargs,
        n_training_epochs=10,
        batch_size=batch_size,
    )
    return embedder.fit(get_sample(data, train_frac)).transform(data)


def pca_reduce_embedding_dimension(
    data,
    dimension,
):
    return PCA(n_components=dimension).fit_transform(data)
