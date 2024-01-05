import pandas as pd
import numpy as np
import json
import io
from google.cloud import bigquery
import umap
import random
from sklearn.decomposition import PCA
import tensorflow as tf
from loguru import logger


def convert_str_emb_to_float(emb_list):
    float_emb = []
    for str_emb in emb_list:
        emb = json.loads(str_emb)
        float_emb.append(np.array(emb))
    return float_emb


def convert_arr_emb_to_str(emb_list):
    float_emb = []
    for str_emb in emb_list:
        emb = json.dumps(str_emb.tolist())
        float_emb.append(emb)
    return float_emb


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
        ]
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


def export_polars_to_bq(data, project_id, dataset, output_table):
    client = bigquery.Client()
    with io.BytesIO() as stream:
        data.write_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            project=project_id,
            destination=f"{dataset}.{output_table}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
    job.result()
