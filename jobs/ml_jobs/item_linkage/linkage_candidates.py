import uuid
from typing import Optional

import numpy as np
import pandas as pd
import typer
from hnne import HNNE
from loguru import logger
from sklearn.preprocessing import normalize
from tqdm import tqdm

from constants import (
    MODEL_PATH,
    NUM_RESULTS,
    SYNCHRO_SUBCATEGORIES,
)
from model.semantic_space import SemanticSpace
from utils.common import (
    preprocess_embeddings_by_chunk,
    read_parquet_in_batches_gcs,
    reduce_embeddings,
)
from utils.gcs_utils import upload_parquet

app = typer.Typer()


def load_model(model_path: str, linkage_type: str, reduction: bool) -> SemanticSpace:
    """
    Load the SemanticSpace model from the given path.

    Args:
        model_path (str): Path to the model.
        reduction (bool): Whether to reduce the embeddings.

    Returns:
        SemanticSpace: The loaded model.
    """
    logger.info("Loading model...")
    model = SemanticSpace(model_path, linkage_type, reduction)
    logger.info("Model loaded.")
    return model


def preprocess_data(
    chunk: pd.DataFrame, hnne_reducer: HNNE, linkage_type: str
) -> pd.DataFrame:
    """
    Preprocess the data by reading the parquet file, filling missing values, and merging embeddings.

    Args:
        chunk (pd.DataFrame): The data to preprocess.
        hnne_reducer (HNNE): The HNNE reducer to use.
        linkage_type (str): Type of linkage to perform

    Returns:
        pd.DataFrame: The preprocessed data.
    """
    logger.info("Preprocessing data...")
    # extract_pattern = r"\b(?:Tome|tome|t|vol|episode|)\s*(\d+)\b"  # This pattern is for extracting the edition number
    # remove_pattern = r"\b(?:Tome|tome|t|vol|episode|)\s*\d+\b"  # This pattern is for removing the edition number and keyword
    if linkage_type == "product":
        chunk = chunk[chunk.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
    # items_df = chunk.assign(
    #     performer=lambda df: df["performer"].fillna(value=UNKNOWN_PERFORMER),
    #     edition=lambda df: df["offer_name"]
    #     .str.extract(extract_pattern, expand=False)
    #     .astype(str)
    #     .fillna(value="1"),
    #     offer_name=lambda df: df["offer_name"]
    #     .str.replace(remove_pattern, "", regex=True)
    #     .str.strip(),
    # ).drop(columns=["embedding"])
    items_df = chunk.drop(columns=["embedding"])
    if hnne_reducer:
        items_df["vector"] = reduce_embeddings(
            preprocess_embeddings_by_chunk(chunk), hnne_reducer=hnne_reducer
        )
    else:
        items_df["vector"] = list(preprocess_embeddings_by_chunk(chunk))
    embeddings_list = items_df["vector"].tolist()

    # Convert embeddings to a NumPy array
    embeddings_array = np.array(embeddings_list)

    # Normalize the embeddings
    normalized_embeddings = normalize(embeddings_array, norm="l2")

    # Update the DataFrame with normalized embeddings
    items_df["vector"] = list(normalized_embeddings)
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
    for index, row in tqdm(
        data.iterrows(), total=data.shape[0], desc="Processing rows", mininterval=60
    ):
        result_df = model.search(
            vector=row.vector,
            offer_subcategory_id=row.offer_subcategory_id,
            edition=row.edition,
            similarity_metric="L2",
            n=NUM_RESULTS,
            vector_column_name="vector",
        ).assign(candidates_id=str(uuid.uuid4()), item_id_candidate=row.item_id)
        linkage.append(result_df)
    return pd.concat(linkage)


@app.command()
def main(
    batch_size: int = typer.Option(default=..., help="Batch size"),
    linkage_type: str = typer.Option(default=..., help="Linkage type"),
    reduction: str = typer.Option(default=..., help="Reduce embeddings"),
    input_path: str = typer.Option(default=..., help="Input table path"),
    output_path: str = typer.Option(default=..., help="Output table path"),
    unmatched_elements_path: Optional[str] = typer.Option(
        default=None, help="Unmatched elements"
    ),
) -> None:
    """
    Main function to preprocess data, prepare vectors, generate semantic candidates, and upload the results to GCS.

    Args:
        batch_size (int): The size of each batch.
        linkage_type (str): The type of linkage to perform.
        reduction (str): Whether to reduce the embeddings.
        input_path (str): The path to the input table.
        output_path (str): The path to the output table.
        unmatched_elements_path (str): The path to the unmatched elements table.
    """
    reduction = True if reduction == "true" else False
    model = load_model(MODEL_PATH, linkage_type, reduction)
    if linkage_type == "offer":
        unmatched_elements = pd.read_parquet(unmatched_elements_path)
        logger.info(f"unmatched_elements: {len(unmatched_elements)} items")
    tqdm.pandas()
    linkage_by_chunk = []
    for chunk in tqdm(read_parquet_in_batches_gcs(input_path, batch_size)):
        if linkage_type == "offer":
            logger.info("Merging unmatched elements...")
            chunk = chunk.merge(
                unmatched_elements[["item_id"]], on="item_id", how="inner"
            )
        if linkage_type == "product":
            chunk = chunk[chunk.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
        logger.info(f"chunk length: {len(chunk)} ")
        linkage_candidates_chunk = generate_semantic_candidates(model, chunk)
        linkage_by_chunk.append(linkage_candidates_chunk)
    linkage_candidates = pd.concat(linkage_by_chunk)
    upload_parquet(dataframe=linkage_candidates, gcs_path=output_path)


if __name__ == "__main__":
    app()
