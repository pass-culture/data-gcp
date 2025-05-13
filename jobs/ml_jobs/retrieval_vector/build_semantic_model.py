import joblib
import numpy as np
import polars as pl
import pyarrow.dataset as ds
import typer
from hnne import HNNE
from loguru import logger

from utils import (
    ENV_SHORT_NAME,
    OUTPUT_DATA_PATH,
    create_items_table,
    get_item_docs,
    get_items_metadata,
    save_model_type,
)

MODEL_TYPE = {
    "type": "semantic",
    "default_token": None,
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
    "reducer": f"{OUTPUT_DATA_PATH}/reducer.pkl",
}
EMBEDDING_DIMENSION = 32


def download_embeddings_and_reduce_dimensions(
    bucket_path: str, reduced_dimension: int
) -> dict:
    """Download embeddings from the specified bucket path."""
    logger.info("Downloading embeddings...")
    dataset = ds.dataset(bucket_path, format="parquet")
    logger.info("Embeddings downloaded.")

    logger.info(f"Reducing embeddings dimensions to {reduced_dimension} with HNNE...")
    hnne = HNNE(dim=reduced_dimension)
    ldf = pl.scan_pyarrow_dataset(dataset)
    item_list = ldf.select("item_id").collect().to_numpy().flatten()
    item_weights = np.vstack(np.vstack(ldf.select("embedding").collect())[0]).astype(
        np.float32
    )
    item_weights = hnne.fit_transform(item_weights, dim=reduced_dimension).astype(
        np.float32
    )
    logger.info("Embedding dimensions reduced.")

    joblib.dump(hnne, MODEL_TYPE["reducer"])

    return {x: y for x, y in zip(item_list, item_weights)}


def prepare_docs(bucket_path: str) -> None:
    logger.info("Get items metadata...")
    items_df = get_items_metadata()
    logger.info("Items metadata loaded.")

    logger.info("Getting semantic embeddings...")
    item_embedding_dict = download_embeddings_and_reduce_dimensions(
        bucket_path=bucket_path, reduced_dimension=EMBEDDING_DIMENSION
    )
    logger.info("Semantic embeddings downloaded and reduced.")

    logger.info("Building item documents...")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save(f"{OUTPUT_DATA_PATH}/item.docs")
    logger.info("Item documents built.")

    logger.info("Create items lancedb table...")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=EMBEDDING_DIMENSION,
        uri=f"{OUTPUT_DATA_PATH}/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )
    logger.info("Items lancedb table created.")


def main(
    source_gs_path: str = typer.Option(
        None,
        help="GCS parquet path",
    ),
) -> None:
    print("Download...")

    prepare_docs(source_gs_path)
    print("Deploy...")
    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)


if __name__ == "__main__":
    typer.run(main)
