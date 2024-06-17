from datetime import datetime
import typer
from utils import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    deploy_container,
    get_items_metadata,
    save_experiment,
    save_model_type,
    get_item_docs,
    create_items_table,
)
import numpy as np
import json
import pyarrow.dataset as ds
import polars as pl
import pandas as pd
import lancedb
from lancedb.pydantic import Vector, LanceModel
from hnne import HNNE
import joblib

# Define constants
MODEL_TYPE = {
    "n_dim": 32,
    "type": "semantic",
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
}
COLUMN_NAME_LIST = ["item_id", "performer"]
DATA_CLEAN_PATH = "./data_clean.parquet"
URI = "./metadata/vector"


def preprocess_embeddings(bucket_path):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    hnne = HNNE(dim=MODEL_TYPE["n_dim"])
    dataset = ds.dataset(bucket_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    item_list = ldf.select("item_id").collect().to_numpy().flatten()
    item_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
        np.float32
    )
    item_weights = list(
        hnne.fit_transform(item_weights, dim=MODEL_TYPE["n_dim"]).astype(np.float32)
    )
    joblib.dump(hnne, MODEL_TYPE["reducer"])

    return pd.DataFrame({"item_id": item_list, "embedding": item_weights})


def prepare_table(clean_path, column_name_list):
    """
    Prepare a table by reading a parquet file and merging it with preprocessed embeddings.

    Parameters:
    clean_path (str): Path to the parquet file.
    column_name_list (list): List of column names to read from the parquet file.

    Returns:
    DataFrame: DataFrame containing the specified columns and preprocessed embeddings.
    """
    item_df = pd.read_parquet(clean_path, columns=column_name_list)
    item_embeddings = preprocess_embeddings(clean_path)
    item_df = item_df.merge(item_embeddings, on="item_id")
    item_df.rename(columns={"name_embedding": "vector"}, inplace=True)
    item_df = item_df[column_name_list + ["vector"]]
    return item_df


# Define functions for saving model and creating table
def save_model_type(model_type):
    with open("./retrieval_vector/metadata/model_type.json", "w") as file:
        json.dump(model_type, file)


def create_items_table(items_df, uri):
    """
    Create a table in the database with the given items dataframe.

    This function first defines a LanceModel with the required fields. It then connects to the database,
    drops any existing database, and creates a new table with the items data. The data is added in batches
    for efficiency. Finally, it creates an index on the table for faster querying.

    Parameters:
    items_df (DataFrame): DataFrame containing the items data.
    uri (str): URI for the database.

    Returns:
    None
    """

    class Item(LanceModel):
        vector: Vector(32)
        item_id: str
        performer: str

    def make_batches():
        for i in range(0, len(items_df), 5000):
            yield pd.DataFrame(items_df[i : i + 5000])

    db = lancedb.connect(uri)
    db.drop_database()
    db.create_table("items", make_batches(), schema=Item)
    table = db.open_table("items")
    table.create_index(num_partitions=1024, num_sub_vectors=32)


def main(
    experiment_name: str = typer.Option(
        "linkage_semantic_vector_v1.0_prod",
        help="Name of the experiment",
    ),
    model_name: str = typer.Option(
        "name_space",
        help="Name of the model",
    ),
    source_gs_path: str = typer.Option(
        "gs://mlflow-bucket-prod/linkage_vector_prod/name_20240618T152206/20240618T152206_item_embbedding_data.parquet",
        help="GCS parquet path",
    ),
) -> None:
    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = (
        f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    )
    print(f"Download...")
    item_df_enriched = prepare_table(source_gs_path, COLUMN_NAME_LIST)

    create_items_table(item_df_enriched, URI)
    print("Deploy...")
    save_model_type(MODEL_TYPE)
    deploy_container(serving_container, workers=3)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)
    print("Deployed.")


if __name__ == "__main__":
    typer.run(main)
