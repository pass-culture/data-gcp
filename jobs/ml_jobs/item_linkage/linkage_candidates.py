import uuid

import pandas as pd
import typer
from constants import LOGGING_INTERVAL, MODEL_PATH, NUM_RESULTS, PARQUET_BATCH_SIZE
from docarray import Document
from hnne import HNNE
from loguru import logger
from model.semantic_space import SemanticSpace
from tqdm import tqdm
from utils.common import (
    preprocess_embeddings_by_chunk,
    read_parquet_in_batches_gcs,
    reduce_embeddings,
)
from utils.gcs_utils import upload_parquet

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


def preprocess_data(chunk: pd.DataFrame, hnne_reducer: HNNE) -> pd.DataFrame:
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
    items_df = chunk.assign(
        performer=lambda df: df["performer"].fillna(value="unkn"),
        offer_name=lambda df: df["offer_name"].str.lower(),
    ).drop(columns=["embedding"])
    items_df["vector"] = reduce_embeddings(
        preprocess_embeddings_by_chunk(chunk), hnne_reducer=hnne_reducer
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
        ).assign(candidates_id=str(uuid.uuid4()), item_id_candidate=row.item_id)
        linkage.append(result_df)
        progress_bar.update(1)
    progress_bar.close()
    return pd.concat(linkage)


@app.command()
def main(
    input_path: str = typer.Option(default=..., help="Input table path"),
    output_path: str = typer.Option(default=..., help="Output table path"),
) -> None:
    """
    Main function to preprocess data, prepare vectors, generate semantic candidates, and upload the results to GCS.

    Args:
        source_gcs_path (str): GCS path to the source data.
        input_table_path (str): Name of the input table.
        output_table_path (str): Path to save the output table.
    """
    model = load_model(MODEL_PATH)

    linkage_by_chunk = []
    for chunk in read_parquet_in_batches_gcs(input_path, PARQUET_BATCH_SIZE):
        items_with_embeddings_df = preprocess_data(chunk, model.hnne_reducer)
        linkage_candidates_chunk = generate_semantic_candidates(
            model, items_with_embeddings_df
        )
        linkage_by_chunk.append(linkage_candidates_chunk)
    linkage_candidates = pd.concat(linkage_by_chunk)
    upload_parquet(dataframe=linkage_candidates, gcs_path=output_path)


if __name__ == "__main__":
    app()
