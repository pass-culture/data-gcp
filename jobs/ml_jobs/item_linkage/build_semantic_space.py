from datetime import datetime
import typer
from utils.common import (
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
from utils.gcs_utils import upload_to_gcs

# Define constants
MODEL_TYPE = {
    "n_dim": 32,
    "type": "semantic",
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
    "reducer": "metadata/reducer.pkl",
}
COLUMN_NAME_LIST = ["item_id", "performer"]
DATA_CLEAN_PATH = "./data_clean.parquet"
URI = "metadata/vector"


def preprocess_embeddings(bucket_path, input_table_name):
    """
    Preprocess embeddings from a given bucket path by normalizing them.

    Parameters:
    bucket_path (str): Path to the bucket containing the embeddings.

    Returns:
    DataFrame: DataFrame containing item IDs and normalized embeddings.
    """
    hnne = HNNE(dim=MODEL_TYPE["n_dim"])
    dataset = ds.dataset(f"{bucket_path}/{input_table_name}", format="parquet")
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


def prepare_table(gcs_path, column_name_list):
    """
    Prepare a table by reading a parquet file and merging it with preprocessed embeddings.

    Parameters:
    clean_path (str): Path to the parquet file.
    column_name_list (list): List of column names to read from the parquet file.

    Returns:
    DataFrame: DataFrame containing the specified columns and preprocessed embeddings.
    """
    item_df = pd.read_parquet(gcs_path, columns=column_name_list)
    item_embeddings = preprocess_embeddings(gcs_path)
    item_df = item_df.merge(item_embeddings, on="item_id")
    item_df.rename(columns={"embedding": "vector"}, inplace=True)
    item_df = item_df[column_name_list + ["vector"]]
    return item_df


# Define functions for saving model and creating table
def save_model_type(model_type):
    with open("metadata/model_type.json", "w") as file:
        json.dump(model_type, file)


def create_items_table(items_df, source_gs_path, uri):
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

    # db = lancedb.connect(f"{source_gs_path}/linkage_vector_prod/name_20240623T040000/")
    db = lancedb.connect(URI)
    # URI
    # db.drop_database()
    db.create_table("items", make_batches(), schema=Item)
    table = db.open_table("items")
    table.create_index(num_partitions=1024, num_sub_vectors=32)


def main(
    source_gcs_path: str = typer.Option(
        "gs://mlflow-bucket-prod/linkage_vector_prod/name_20240623T040000/",
        help="GCS parquet path",
    ),
    input_table_name: str = typer.Option(
        "item_data",
        help="GCS parquet path",
    ),
    output_table_path: str = typer.Option(
        "metadata/vector",
        help="GCS parquet path",
    ),
) -> None:
    print("Download and prepare table...")
    item_df_enriched = prepare_table(
        f"{source_gs_path}/{input_table_name}", COLUMN_NAME_LIST
    )
    logger.info("Creating LanceDB table...")
    create_items_table(item_df_enriched, source_gs_path, output_table_name)
    save_model_type(MODEL_TYPE)
    logger.info("Creating LanceDB table and index created...")


if __name__ == "__main__":
    typer.run(main)
