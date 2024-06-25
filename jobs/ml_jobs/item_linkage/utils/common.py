import json
import os

import joblib
import numpy as np
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
from hnne import HNNE

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


def preprocess_embeddings_and_store_reducer(gcs_path, n_dim, reducer_path):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    hnne = HNNE(dim=n_dim)
    dataset = ds.dataset(gcs_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    item_list = ldf.select("item_id").collect().to_numpy().flatten()
    item_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
        np.float32
    )
    item_weights = list(hnne.fit_transform(item_weights, dim=n_dim).astype(np.float32))
    joblib.dump(hnne, reducer_path)

    return pd.DataFrame({"item_id": item_list, "embedding": item_weights})
