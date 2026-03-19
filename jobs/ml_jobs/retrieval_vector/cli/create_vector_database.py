import os

from src.embeddings import extract_embeddings_from_tt_model, generate_dummy_embeddings
from src.vector_database import (
    create_lancedb_from_coreservation,
    create_lancedb_from_item_embeddings,
)

################################  To use Keras 2 instead of 3  ################################
# See [TensorFlow + Keras 2 backwards compatibility section](https://keras.io/getting_started/)
os.environ["TF_USE_LEGACY_KERAS"] = "1"
###############################################################################################

import pandas as pd
import typer
from loguru import logger

from src.constants import MODEL_BASE_PATH, OUTPUT_DATA_PATH
from src.gcs_io import (
    download_model,
    get_items_metadata,
    get_model_from_mlflow,
    get_users_dummy_metadata,
    load_embeddings_from_parquet,
)
from src.utils import (
    save_model_type,
)

app = typer.Typer(help="Create lanceDB table and documents")


@app.command()
def dummy_database() -> None:
    MODEL_TYPE = {
        "type": "recommendation",
        "default_token": "[UNK]",
    }
    EMBEDDING_DIMENSION = 16

    # Load data
    logger.info("Load items and dummy user metadata...")
    items_df = get_items_metadata()
    user_df = get_users_dummy_metadata()
    logger.info("Items and dummy user metadata loaded.")

    # Generate dummy embeddings
    logger.info("Generate dummy embeddings...")
    user_embedding_dict, item_embedding_dict = generate_dummy_embeddings(
        embedding_dimension=EMBEDDING_DIMENSION,
        item_ids=items_df.item_id.tolist(),
        user_ids=user_df.user_id.tolist(),
        default_token=MODEL_TYPE["default_token"],
    )
    logger.info("Dummy embeddings generated.")

    # Build dummy lanceDB table and documents
    logger.info("Building dummy lanceDB table and documents...")
    create_lancedb_from_coreservation(
        user_embedding_dict=user_embedding_dict,
        item_embedding_dict=item_embedding_dict,
        item_metadatas_df=items_df,
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

    # Load model and generate embeddings
    # TODO: Refacto this to read embeddings directly from bigquery
    logger.info(f"Download model from {source_artifact_uri} trained model...")
    download_model(artifact_uri=source_artifact_uri)
    logger.info("Model downloaded.")
    logger.info("Extract embeddings from trained model...")
    user_embedding_dict, item_embedding_dict = extract_embeddings_from_tt_model(
        model_path=MODEL_BASE_PATH
    )
    logger.info("Embeddings extracted from trained model.")

    # Load items metadata
    logger.info("Get items metadata...")
    items_df = get_items_metadata()
    logger.info("Items metadata loaded.")

    # build user and item documents
    logger.info("Building lanceDB table and documents...")
    create_lancedb_from_coreservation(
        user_embedding_dict=user_embedding_dict,
        item_embedding_dict=item_embedding_dict,
        item_metadatas_df=items_df,
    )
    logger.info("LanceDB table and documents built.")

    # Output model type
    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)
    logger.info(f"Model type ({MODEL_TYPE['type']}) saved.")


@app.command()
def graph_database(
    recommendable_item_gs_path: str = typer.Option(
        ...,
        help="Path (GCS or local) to the parquet or parquet dir containing the items with metadatas to recommend",
    ),
    graph_embedding_gs_path: str = typer.Option(
        ...,
        help="Path (GCS or local) to the parquet or parquet dir containing the item graph embeddings",
    ),
) -> None:
    MODEL_TYPE = {
        "type": "metadata_graph",
        "default_token": None,
    }
    # Load data
    logger.info("Load items with metadatas and graph embeddings...")
    recommendable_items_df = pd.read_parquet(recommendable_item_gs_path)
    graph_embeddings_df = load_embeddings_from_parquet(
        graph_embedding_gs_path,
        column_renaming_mapping={"node_ids": "item_id", "embedding": "vector"},
    )
    items_with_embeddings_df = recommendable_items_df.merge(
        graph_embeddings_df, on="item_id", how="inner", validate="one_to_one"
    )
    logger.info("Items with metadatas and graph embeddings loaded.")

    # build item documents and lanceDB table
    logger.info("Building lanceDB table and documents...")
    create_lancedb_from_item_embeddings(items_with_embeddings_df)
    logger.info("LanceDB table and documents built.")

    # Output model type
    save_model_type(model_type=MODEL_TYPE, output_dir=OUTPUT_DATA_PATH)
    logger.info(f"Model type ({MODEL_TYPE['type']}) saved.")


if __name__ == "__main__":
    app()
