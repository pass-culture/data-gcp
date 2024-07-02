import json
import os

import joblib
import numpy as np
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
from hnne import HNNE
from loguru import logger

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

item_columns = [
    "vector",
    "item_id",
    "performer" "offer_name",
]


def save_model_type(model_type):
    with open("metadata/model_type.json", "w") as file:
        json.dump(model_type, file)


def reduce_embeddings_and_store_reducer(embeddings: list, n_dim, reducer_path):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Reducing embeddings and storing reducer...")
    hnne = HNNE(dim=n_dim)
    reduced_embeddings = list(
        hnne.fit_transform(embeddings, dim=n_dim).astype(np.float32)
    )

    joblib.dump(hnne, reducer_path)

    # return pd.DataFrame({"embedding":reduced_embeddings})
    return reduced_embeddings


def reduce_embeddings(embeddings: list, hnne_reducer: HNNE) -> list:
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Reducing embeddings...")
    return list(hnne_reducer.transform(embeddings).astype(np.float32))


def preprocess_embeddings(
    gcs_path,
):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    logger.info("Loading embeddings...")
    dataset = ds.dataset(gcs_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    embedding = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
        np.float32
    )
    return embedding
