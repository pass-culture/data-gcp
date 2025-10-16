import joblib
import numpy as np
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import tensorflow as tf
import typer
from docarray import Document, DocumentArray
from hnne import HNNE
from loguru import logger

from utils import (
    ENV_SHORT_NAME,
    MODEL_BASE_PATH,
    OUTPUT_DATA_PATH,
    create_items_table,
    download_model,
    get_item_docs,
    get_items_metadata,
    get_model_from_mlflow,
    get_user_docs,
    get_users_dummy_metadata,
    save_model_type,
)

app = typer.Typer(help="Create lanceDB table and documents")


def prepare_docs(embedding_dimension: int) -> None:
    logger.info("Get items metadata...")
    items_df = get_items_metadata()
    logger.info("Items metadata loaded.")

    # download model
    logger.info("Load Two Tower model...")
    tf_reco = tf.keras.models.load_model(MODEL_BASE_PATH)
    logger.info("Two Tower model loaded.")

    # get user and item embeddings
    item_list = tf_reco.item_layer.layers[0].get_vocabulary()
    item_weights = tf_reco.item_layer.layers[1].get_weights()[0].astype(np.float32)
    user_list = tf_reco.user_layer.layers[0].get_vocabulary()
    user_weights = tf_reco.user_layer.layers[1].get_weights()[0].astype(np.float32)
    user_embedding_dict = {x: y for x, y in zip(user_list, user_weights)}
    item_embedding_dict = {x: y for x, y in zip(item_list, item_weights)}

    # build user and item documents
    logger.info("Building user and item documents...")
    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save(f"{OUTPUT_DATA_PATH}/user.docs")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save(f"{OUTPUT_DATA_PATH}/item.docs")
    logger.info("User and item documents built.")

    logger.info("Create items lancedb table...")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=embedding_dimension,
        uri=f"{OUTPUT_DATA_PATH}/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )
    logger.info("Items lancedb table created.")


def prepare_dummy_docs(embedding_dimension: int, default_token: str) -> None:
    """Prepare dummy documents and lanceDB table."""
    logger.info("Get items...")
    items_df = get_items_metadata()
    user_df = get_users_dummy_metadata()
    # default
    user_embedding_dict = {
        row.user_id: np.random.random((embedding_dimension,))
        for row in user_df.itertuples()
    }
    user_embedding_dict[default_token] = np.random.random(
        embedding_dimension,
    )
    item_embedding_dict = {
        row.item_id: np.random.random((embedding_dimension,))
        for row in items_df.itertuples()
    }
    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save(f"{OUTPUT_DATA_PATH}/user.docs")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save(f"{OUTPUT_DATA_PATH}/item.docs")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=embedding_dimension,
        uri=f"{OUTPUT_DATA_PATH}/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )


def download_embeddings_and_reduce_dimensions(
    bucket_path: str, reduced_dimension: int, reducer_output_path: str = None
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

    joblib.dump(hnne, reducer_output_path)

    return {x: y for x, y in zip(item_list, item_weights)}


def prepare_semantic_docs(
    bucket_path: str, embedding_dimension: int, reducer_output_path: str
) -> None:
    logger.info("Get items metadata...")
    items_df = get_items_metadata()
    logger.info("Items metadata loaded.")

    logger.info("Getting semantic embeddings...")
    item_embedding_dict = download_embeddings_and_reduce_dimensions(
        bucket_path=bucket_path,
        reduced_dimension=embedding_dimension,
        reducer_output_path=reducer_output_path,
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
        emb_size=embedding_dimension,
        uri=f"{OUTPUT_DATA_PATH}/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )
    logger.info("Items lancedb table created.")


def create_graph_item_docs(items_with_embeddings_df: pd.DataFrame, output_path: str):
    """
    Create and save item documents for graph retrieval.

    Builds DocumentArray from items with graph embeddings and saves them to disk.
    The embeddings are extracted from the 'vector' column, while other columns
    contain item metadata.

    Args:
        items_with_embeddings_df (pd.DataFrame): DataFrame containing items with their
            graph embeddings in a 'vector' column and associated metadata in other columns.
        output_path: Path to save the item docs

    Returns:
        None: Saves item documents to the provided output_path
    """
    item_docs = DocumentArray(
        [
            Document(id=row.id, embedding=row.vector)
            for row in items_with_embeddings_df.itertuples()
        ]
    )
    item_docs.save(output_path)


@app.command()
def semantic_database(
    source_gs_path: str = typer.Option(
        None,
        help="GCS parquet path",
    ),
) -> None:
    MODEL_TYPE = {
        "type": "semantic",
        "default_token": None,
        "transformer": "sentence-transformers/all-MiniLM-L6-v2",
        "reducer": f"{OUTPUT_DATA_PATH}/reducer.pkl",
    }
    EMBEDDING_DIMENSION = 32

    prepare_semantic_docs(
        source_gs_path,
        embedding_dimension=EMBEDDING_DIMENSION,
        reducer_output_path=MODEL_TYPE["reducer"],
    )
    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)


@app.command()
def graph_database(
    recommendable_item_gs_path: str = typer.Option(
        ...,
        help="Path (GCS or local) to the parquet or parquet dir containing the items with metadatas to recommend",
    ),
    graph_embedding_path: str = typer.Option(
        ...,
        help="Path (GCS or local) to the parquet or parquet dir containing the item graph embeddings",
    ),
) -> None:
    MODEL_TYPE = {
        "type": "graph_retrieval",
        "default_token": None,
    }
    # Load data
    recommendable_items_df = pd.read_parquet(recommendable_item_gs_path)
    graph_embeddings_df = pd.read_parquet(graph_embedding_path)
    items_with_embeddings_df = recommendable_items_df.merge(
        graph_embeddings_df, on="item_id", how="inner"
    ).rename(columns={"embedding": "vector"})

    # Create item documents for graph retrieval app
    create_graph_item_docs(items_with_embeddings_df=items_with_embeddings_df)

    # Create lanceDB table for graph retrieval
    create_items_table(
        item_embedding_dict={
            row.item_id: row.vector for row in items_with_embeddings_df.itertuples()
        },
        items_df=items_with_embeddings_df.loc[lambda df: df.columns != "vector"],
        emb_size=len(items_with_embeddings_df.iloc[0].vector),
        uri=f"{OUTPUT_DATA_PATH}/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )

    # Output model type
    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)


@app.command()
def dummy_database() -> None:
    MODEL_TYPE = {
        "type": "recommendation",
        "default_token": "[UNK]",
    }
    EMBEDDING_DIMENSION = 16

    logger.info("Building dummy lanceDB table, and dummy user and item docarrays...")
    prepare_dummy_docs(
        embedding_dimension=EMBEDDING_DIMENSION,
        default_token=MODEL_TYPE["default_token"],
    )
    logger.info("Dummy lanceDB table and documents built.")

    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)
    logger.info("Model type saved.")


@app.command()
def default_database(
    source_experiment_name: str = typer.Option(
        None,
        help="Source of the experiment",
    ),
    source_artifact_uri: str = typer.Option(
        None,
        help="Source artifact_uri of the model",
    ),
    source_run_id: str = typer.Option(
        None,
        help="Source run_id of the model",
    ),
) -> None:
    MODEL_TYPE = {
        "type": "recommendation",
        "default_token": "[UNK]",
    }
    EMBEDDING_DIMENSION = 64

    if source_artifact_uri is None or len(source_artifact_uri) <= 10:
        logger.info(
            f"Get model from MLFlow experiment {source_experiment_name} with run_id {source_run_id}..."
        )
        source_artifact_uri = get_model_from_mlflow(
            experiment_name=source_experiment_name,
            run_id=source_run_id,
            artifact_uri=source_artifact_uri,
        )
        logger.info(f"Model artifact_uri: {source_artifact_uri}")

    logger.info(f"Download model from {source_artifact_uri} trained model...")
    download_model(artifact_uri=source_artifact_uri)
    logger.info("Model downloaded.")

    logger.info("Building lanceDB table, and user and item docarrays...")
    prepare_docs(embedding_dimension=EMBEDDING_DIMENSION)
    logger.info("LanceDB table and documents built.")

    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)
    logger.info("Model type saved.")


if __name__ == "__main__":
    app()
