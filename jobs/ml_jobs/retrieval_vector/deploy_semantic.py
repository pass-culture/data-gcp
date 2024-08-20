from datetime import datetime

import joblib
import numpy as np
import polars as pl
import pyarrow.dataset as ds
import typer
from hnne import HNNE
import psutil

from utils import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    create_items_table,
    deploy_container,
    get_item_docs,
    get_items_metadata,
    save_experiment,
    save_model_type,
)

MODEL_TYPE = {
    "n_dim": 32,
    "type": "semantic",
    "default_token": None,
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
    "reducer": "./metadata/reducer.pkl",
}


def get_ram_info():
    # Get memory details
    memory_info = psutil.virtual_memory()

    # Available memory in bytes
    available_memory_bytes = memory_info.available

    # Convert to GB for readability
    available_memory_gb = available_memory_bytes / (1024**3)

    print(f"Available memory: {available_memory_gb:.2f} GB")


def download_embeddings(bucket_path):
    # download
    print("start")
    get_ram_info()
    HNNE(dim=MODEL_TYPE["n_dim"])
    dataset = ds.dataset(bucket_path, format="parquet")
    print("after dataset")
    get_ram_info()
    ldf = pl.scan_pyarrow_dataset(dataset)
    print("after scan")
    get_ram_info()
    ldf.select("item_id").collect().to_numpy().flatten()
    print("after list")
    get_ram_info()
    # item_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
    #     np.float32
    # )
    # item_weights = hnne.fit_transform(item_weights, dim=MODEL_TYPE["n_dim"]).astype(
    #     np.float32
    # )
    # joblib.dump(hnne, MODEL_TYPE["reducer"])

    # return {x: y for x, y in zip(item_list, item_weights)}


def prepare_docs(bucket_path):
    print("Get items...")
    items_df = get_items_metadata()
    print("Get embeddings...")
    item_embedding_dict = download_embeddings(bucket_path)
    print("Preproc items...")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save("./metadata/item.docs")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=MODEL_TYPE["n_dim"],
        uri="./metadata/vector",
    )


def main(
    experiment_name: str = typer.Option(
        None,
        help="Name of the experiment",
    ),
    model_name: str = typer.Option(
        None,
        help="Name of the model",
    ),
    source_gs_path: str = typer.Option(
        None,
        help="GCS parquet path",
    ),
) -> None:
    # yyyymmdd = datetime.now().strftime("%Y%m%d")
    # if model_name is None:
    #     model_name = "default"
    # run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    # serving_container = (
    #     f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    # )
    print("Download...")

    prepare_docs(source_gs_path)
    # print("Deploy...")
    # save_model_type(model_type=MODEL_TYPE)
    # deploy_container(serving_container, workers=3)
    # save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
