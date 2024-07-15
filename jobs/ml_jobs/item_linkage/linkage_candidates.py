import uuid
from typing import List

import pandas as pd
import typer
from hnne import HNNE
from loguru import logger
from model.semantic_space import SemanticSpace
from utils.common import preprocess_embeddings, reduce_embeddings
from utils.gcs_utils import upload_parquet
from docarray import Document
from tqdm import tqdm
from constants import LOGGING_INTERVAL, NUM_RESULTS, MODEL_PATH

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
    logger.info("Model loaded.")
    return model


def preprocess_data(
    gcs_path: str, column_list: List[str], hnne_reducer: HNNE
) -> pd.DataFrame:
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
    items_df = pd.read_parquet(gcs_path, columns=column_list).assign(
        performer=lambda df: df["performer"].fillna(value="unkn"),
        offer_name=lambda df: df["offer_name"].str.lower(),
    )
    items_df["vector"] = reduce_embeddings(
        preprocess_embeddings(gcs_path), hnne_reducer=hnne_reducer
    )
    items_df["vector"] = (
        items_df["vector"]
        .map(lambda embedding_array: Document(embedding=embedding_array))
        .tolist()
    )
    return items_df


def generate_semantic_candidates(
    model: SemanticSpace, data: pd.DataFrame
) -> pd.DataFrame:
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
    progress_bar = tqdm(total=len(data), miniters=LOGGING_INTERVAL)
    for index, row in data.iterrows():
        result_df = model.search(
            vector=row.vector,
            similarity_metric="cosine",
            n=NUM_RESULTS,
            vector_column_name="vector",
        ).assign(candidates_id=str(uuid.uuid4()), item_id=row.item_id)

        linkage.append(result_df)
        progress_bar.update(1)
    progress_bar.close()
    return pd.concat(linkage)


@app.command()
def main(
    source_gcs_path: str = typer.Option(
        "gs://mlflow-bucket-prod/linkage_vector_prod", help="GCS bucket path"
    ),
    input_table_name: str = typer.Option(
        "item_candidates_data", help="Input table path"
    ),
    output_table_path: str = typer.Option(
        "linkage_candidates_items", help="Output table path"
    ),
) -> None:
    """
    Main function to preprocess data, prepare vectors, generate semantic candidates, and upload the results to GCS.

    Args:
        source_gcs_path (str): GCS path to the source data.
        input_table_name (str): Name of the input table.
        output_table_path (str): Path to save the output table.
    """
    model = load_model(MODEL_PATH)
    items_with_embeddings_df = preprocess_data(
        f"{source_gcs_path}/{input_table_name}", COLUMN_NAME_LIST, model.hnne_reducer
    )

    linkage_candidates = generate_semantic_candidates(model, items_with_embeddings_df)
    upload_parquet(
        dataframe=linkage_candidates,
        gcs_path=f"{source_gcs_path}/{output_table_path}.parquet",
    )


if __name__ == "__main__":
    app()