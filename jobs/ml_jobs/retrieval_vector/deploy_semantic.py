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
)
import pyarrow.dataset as ds
import polars as pl
import numpy as np

MODEL_TYPE = {
    "n_dim": 382,
    "metric": "cosine",
    "type": "semantic",
    "default_token": None,
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
}


def download_embeddings(bucket_path):
    # download
    dataset = ds.dataset(bucket_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    item_list = ldf.select("item_id").collect().to_numpy().flatten()
    item_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
        np.float32
    )
    return {x: y for x, y in zip(item_list, item_weights)}


def prepare_docs(bucket_path, model_type):
    items_df = get_items_metadata()
    item_embedding_dict = download_embeddings(bucket_path)
    item_docs = get_item_docs(
        item_embedding_dict, items_df, model_type["n_dim"], model_type["metric"]
    )
    item_docs.save("./metadata/item.docs")


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
    print(f"Download...")
    print("Deploy...")
    prepare_docs(source_gs_path, model_type=MODEL_TYPE)

    save_model_type(model_type=MODEL_TYPE)
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
