import joblib
import numpy as np
import polars as pl
import pyarrow.dataset as ds
import typer
from hnne import HNNE

from utils import (
    ENV_SHORT_NAME,
    create_items_table,
    get_item_docs,
    get_items_metadata,
    save_model_type,
)

MODEL_TYPE = {
    "type": "semantic",
    "default_token": None,
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
    "reducer": "./metadata/reducer.pkl",
}
EMBEDDING_DIMENSION = 32


def download_embeddings(bucket_path):
    # download
    hnne = HNNE(dim=EMBEDDING_DIMENSION)
    dataset = ds.dataset(bucket_path, format="parquet")
    ldf = pl.scan_pyarrow_dataset(dataset)
    item_list = ldf.select("item_id").collect().to_numpy().flatten()
    item_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
        np.float32
    )
    item_weights = hnne.fit_transform(item_weights, dim=EMBEDDING_DIMENSION).astype(
        np.float32
    )
    joblib.dump(hnne, MODEL_TYPE["reducer"])

    return {x: y for x, y in zip(item_list, item_weights)}


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
        emb_size=EMBEDDING_DIMENSION,
        uri="./metadata/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )


def main(
    source_gs_path: str = typer.Option(
        None,
        help="GCS parquet path",
    ),
) -> None:
    print("Download...")

    prepare_docs(source_gs_path)
    print("Deploy...")
    save_model_type(model_type=MODEL_TYPE)


if __name__ == "__main__":
    typer.run(main)
