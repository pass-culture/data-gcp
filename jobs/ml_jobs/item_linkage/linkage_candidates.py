import json
import uuid
from typing import Dict, Tuple

import pandas as pd
import typer
from docarray import Document
from loguru import logger
from lancedb.pydantic import Vector, LanceModel
from google.cloud import storage
from urllib.parse import urlparse

from model.semantic_space import SemanticSpace
from utils.gcs_utils import upload_parquet, read_parquet, download_to_filename_from_gcs

# Constants
LOCAL_RETRIEVAL_PATH = "./linkage_vector"
NUM_RESULTS = 40  # Replacing hardcoded value
LOGGING_INTERVAL = 10000  # Replacing hardcoded value

app = typer.Typer()


class Item(LanceModel):
    vector: Vector(32)
    item_id: str
    performer: str


def load_model(model_path: str, retrieval_path: str) -> SemanticSpace:
    """Load the model from the given GCS path.

    Args:
        bucket_gcs_path (str): The GCS path to the model.
        retrieval_path (str): The local path to save the model files.

    Returns:
        SemanticSpace: The loaded model.
    """
    logger.info("Loading model...")
    model = SemanticSpace(model_path)
    model.load()
    logger.info("Model loaded.")
    return model


def get_semantic_candidates(
    model: SemanticSpace,
    vector: Vector,
    params: Dict,
    metric: str,
    k: int = NUM_RESULTS,
) -> pd.DataFrame:
    """Get semantic candidates for the given vector.

    Args:
        model (SemanticSpace): The model to use for getting semantic candidates.
        vector (Vector): The vector to get semantic candidates for.
        params (Dict): The parameters to use for getting semantic candidates.
        metric (str): The metric to use for getting semantic candidates.
        k (int, optional): The number of semantic candidates to get. Defaults to NUM_RESULTS.

    Returns:
        pd.DataFrame: The semantic candidates.
    """
    return model.search(
        vector=vector,
        similarity_metric=metric,
        n=k,
        query_filter=params,
        details=True,
        prefilter=True,
        vector_column_name="vector",
    )


def update_dataframe(df: pd.DataFrame, item_id: str) -> pd.DataFrame:
    """Update the dataframe with the given item_id.

    Args:
        df (pd.DataFrame): The dataframe to update.
        item_id (str): The item_id to update the dataframe with.

    Returns:
        pd.DataFrame: The updated dataframe.
    """
    df["source_item_id"] = [item_id] * len(df)
    df["batch_id"] = [str(uuid.uuid4())[:5]] * len(df)
    df.rename(
        columns={"item_id": "link_item_id", "source_item_id": "item_id"}, inplace=True
    )
    return df


def get_vector(model: SemanticSpace, input_text: str) -> Vector:
    """Get the vector for the given input_text.

    Args:
        model (SemanticSpace): The model to use for getting the vector.
        input_text (str): The input_text to get the vector for.

    Returns:
        Vector: The vector for the given input_text.
    """
    return model.text_vector(input_text)


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the given data.

    Args:
        data (pd.DataFrame): The data to preprocess.

    Returns:
        pd.DataFrame: The preprocessed data.
    """
    logger.info("Preprocessing data...")
    data["performer"] = data["performer"].fillna(value="unkn")
    data["offer_name"] = data["offer_name"].str.lower()
    logger.info("Data preprocessed.")
    return data


def prepare_vectors(model: SemanticSpace, data: pd.DataFrame) -> pd.DataFrame:
    """Prepare vectors for the given data.

    Args:
        model (SemanticSpace): The model to use for preparing vectors.
        data (pd.DataFrame): The data to prepare vectors for.

    Returns:
        pd.DataFrame: The data with prepared vectors.
    """
    logger.info("Preparing vectors...")
    data_w_emb = data[data["embedding"].notnull()].copy()
    data_wo_emb = data[data["embedding"].isnull()].copy()
    data_w_emb["vector"] = (
        data["embedding"].map(lambda x: Document(embedding=x)).tolist()
    )
    data_wo_emb["vector"] = (
        data_wo_emb["offer_name"].map(lambda x: get_vector(model, str(x))).tolist()
    )
    logger.info("Vectors prepared.")
    return pd.concat([data_w_emb, data_wo_emb], ignore_index=True)


def generate_semantic_candidates(
    model: SemanticSpace, data: pd.DataFrame
) -> pd.DataFrame:
    """Generate semantic candidates for the given data.

    Args:
        model (SemanticSpace): The model to use for generating semantic candidates.
        data (pd.DataFrame): The data to generate semantic candidates for.

    Returns:
        pd.DataFrame: The semantic candidates.
    """
    linkage = []
    logger.info(f"Generating semantic candidates for {len(data)} items...")
    for index, row in data.iterrows():
        # logger.info(f"Processing on going... ({index} already processed)")
        # logger.info(f"row.vector: {row.vector} processing")
        if index % LOGGING_INTERVAL == 0 and index != 0:
            logger.info(f"Processing on going... ({index} already processed)")
        params = {}
        result_df = get_semantic_candidates(model, row.vector, params, "cosine", k=5)
        # if len(result_df) == 0:
        #     # logger.info("!! Warning No candidates !!")
        #     pass
        result_df = update_dataframe(result_df, row.item_id)
        linkage.append(result_df)

    df_linkage = pd.concat(linkage)
    return df_linkage


@app.command()
def main(
    model_path: str = typer.Option("metadata/vector", help="GCS bucket path"),
    gcs_directory_path: str = typer.Option(
        "gs://mlflow-bucket-prod/linkage_vector_prod", help="GCS bucket path"
    ),
    input_table_name: str = typer.Option(
        "item_embedding_data", help="Input table path"
    ),
    output_file_path: str = typer.Option(
        "linkage_candidates_items", help="Output table path "
    ),
) -> None:
    """Main function to execute the script.

    Args:
        bucket_gs_path (str): The GCS bucket path.
        source_file_path (str): The input table path.
        output_file_path (str): The output table path.
        experiment_name (str): The model name to load.
        run_id (str): The run ID of the model.
    """
    data = pd.read_parquet(f"{gcs_directory_path}/{input_table_name}")
    model = load_model(model_path, LOCAL_RETRIEVAL_PATH)
    data_clean = preprocess_data(data)
    data_ready = prepare_vectors(model, data_clean)
    linkage_candidates = generate_semantic_candidates(model, data_ready)
    linkage_candidates.to_parquet("linkage_candidates_items_1806204.parquet")
    upload_parquet(
        dataframe=linkage_candidates,
        gcs_path=f"{gcs_directory_path}/{output_file_path}.parquet",
    )


if __name__ == "__main__":
    app()
