import pandas as pd
from datetime import datetime
import subprocess
import typer
import subprocess
from utils import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    deploy_container,
    get_items_metadata,
    save_experiment,
)
import pyarrow.dataset as ds
import polars as pl
import numpy as np
import umap


def download_embeddings(bucket_path):
    # download
    dataset = ds.dataset(bucket_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    item_list = ldf.select("item_id").collect().to_numpy().flatten()
    model_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0])
    # reduce
    trans = umap.UMAP(
        n_neighbors=4, n_components=64, random_state=42, verbose=True
    ).fit(model_weights)
    umap_list = trans.embedding_
    # save
    np.save("./metadata/weights.npy", umap_list, allow_pickle=True)
    np.save("./metadata/items.npy", item_list, allow_pickle=True)


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

    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = (
        f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    )
    print("Get items metadata...")
    get_items_metadata()
    print(f"Download...")
    download_embeddings(source_gs_path)
    print("Deploy...")
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
