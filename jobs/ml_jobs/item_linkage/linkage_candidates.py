import uuid
import pandas as pd
import typer
import json
from docarray import Document
from loguru import logger
from typing import List

from model.semantic_space import SemanticSpace
from utils.gcs_utils import upload_parquet
from utils.common import preprocess_embeddings

# Constants
NUM_RESULTS = 5  # Number of results to retrieve
LOGGING_INTERVAL = 10000  # Interval for logging progress
COLUMN_NAME_LIST = ["item_id", "performer", "offer_name"]

app = typer.Typer()

def load_model(model_path: str) -> SemanticSpace:
    """
    Load the SemanticSpace model from the given path.

    Args:
        model_path (str): Path to the model.

    Returns:
        SemanticSpace: The loaded model.
    """
    logger.info("Loading model...")
    model = SemanticSpace(model_path)
    model.load()
    logger.info("Model loaded.")
    return model

def update_dataframe(df: pd.DataFrame, item_id: str) -> pd.DataFrame:
    """
    Update the dataframe with the given item_id.

    Args:
        df (pd.DataFrame): Dataframe to update.
        item_id (str): Item ID to update the dataframe with.

    Returns:
        pd.DataFrame: The updated dataframe.
    """
    df["source_item_id"] = [item_id] * len(df)
    df["batch_id"] = [str(uuid.uuid4())[:5]] * len(df)
    df.rename(columns={"item_id": "link_item_id", "source_item_id": "item_id"}, inplace=True)
    return df

def preprocess_data(gcs_path: str, column_list: List[str], model_type: dict) -> pd.DataFrame:
    """
    Preprocess the data by reading the parquet file, filling missing values, and merging embeddings.

    Args:
        gcs_path (str): Path to the GCS parquet file.
        column_list (List[str]): List of columns to read from the parquet file.
        model_type (dict): Model configuration.

    Returns:
        pd.DataFrame: The preprocessed data.
    """
    logger.info("Preprocessing data...")
    data = pd.read_parquet(gcs_path, columns=column_list)
    data["performer"] = data["performer"].fillna(value="unkn")
    data["offer_name"] = data["offer_name"].str.lower()
    item_embeddings = preprocess_embeddings(gcs_path, model_type)
    data = data.merge(item_embeddings, on="item_id")
    data = data[column_list + ["embedding"]]
    logger.info("Data preprocessed.")
    return data

def prepare_vectors(model: SemanticSpace, data: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare vectors for the given data using the SemanticSpace model.

    Args:
        model (SemanticSpace): The model to use for preparing vectors.
        data (pd.DataFrame): The data to prepare vectors for.

    Returns:
        pd.DataFrame: The data with prepared vectors.
    """
    logger.info("Preparing vectors...")
    data_w_emb = data[data["embedding"].notnull()].copy()
    data_wo_emb = data[data["embedding"].isnull()].copy()
    data_w_emb["vector"] = data["embedding"].map(lambda x: Document(embedding=x)).tolist()
    data_wo_emb["vector"] = data_wo_emb["offer_name"].map(lambda x: model.text_vector(str(x)).tolist())
    logger.info("Vectors prepared.")
    return pd.concat([data_w_emb, data_wo_emb], ignore_index=True)

def generate_semantic_candidates(model: SemanticSpace, data: pd.DataFrame) -> pd.DataFrame:
    """
    Generate semantic candidates for the given data using the SemanticSpace model.

    Args:
        model (SemanticSpace): The model to use for generating semantic candidates.
        data (pd.DataFrame): The data to generate semantic candidates for.

    Returns:
        pd.DataFrame: The semantic candidates.
    """
    linkage = []
    logger.info(f"Generating semantic candidates for {len(data)} items...")
    for index, row in data.iterrows():
        if index % LOGGING_INTERVAL == 0 and index != 0:
            logger.info(f"Processing ongoing... ({index} items processed)")
        params = {}
        result_df = model.search(
            vector=row.vector,
            similarity_metric="cosine",
            n=NUM_RESULTS,
            query_filter=params,
            details=True,
            prefilter=True,
            vector_column_name="vector",
        )
        result_df = update_dataframe(result_df, row.item_id)
        linkage.append(result_df)
        if index == 10000:
            break
    df_linkage = pd.concat(linkage)
    return df_linkage

@app.command()
def main(
    model_path: str = typer.Option("metadata/vector", help="Model path"),
    source_gcs_path: str = typer.Option(
        "gs://mlflow-bucket-prod/linkage_vector_prod", help="GCS bucket path"
    ),
    input_table_name: str = typer.Option(
        "item_embedding_data", help="Input table path"
    ),
    output_table_path: str = typer.Option(
        "linkage_candidates_items", help="Output table path"
    ),
) -> None:
    """
    Main function to preprocess data, prepare vectors, generate semantic candidates, and upload the results to GCS.

    Args:
        model_path (str): Path to the model.
        source_gcs_path (str): GCS path to the source data.
        input_table_name (str): Name of the input table.
        output_table_path (str): Path to save the output table.
    """
    with open("metadata/model_type.json", "r") as file:
        config = json.load(file)
    model = load_model(model_path)
    data_clean = preprocess_data(f"{source_gcs_path}/{input_table_name}", COLUMN_NAME_LIST, config)
    data_ready = prepare_vectors(model, data_clean)
    linkage_candidates = generate_semantic_candidates(model, data_ready)
    upload_parquet(
        dataframe=linkage_candidates,
        gcs_path=f"{source_gcs_path}/{output_table_path}.parquet",
    )

if __name__ == "__main__":
    app()
