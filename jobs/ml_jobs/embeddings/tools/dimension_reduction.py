import pandas as pd
import numpy as np
import json
from loguru import logger
import io
from google.cloud import bigquery
import umap
import random
from sklearn.decomposition import PCA


def convert_str_emb_to_float(emb_list):
    float_emb = []
    for str_emb in emb_list:
        emb = json.loads(str_emb)
        float_emb.append(np.array(emb))
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
        metric="cosine",
        low_memory=True,
        unique=True,
    ).fit_transform(data)


def pumap_reduce_embedding_dimension(data, dimension, train_frac=0.1, batch_size=2048):
    return (
        umap.ParametricUMAP(n_components=dimension, batch_size=batch_size)
        .fit(get_sample(data, train_frac))
        .transform(data)
    )


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
