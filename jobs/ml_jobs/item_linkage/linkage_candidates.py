import uuid

import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

from constants import MODEL_PATH, NUM_RESULTS, RETRIEVAL_FILTERS
from model.semantic_space import SemanticSpace
from utils.common import (
    read_parquet_in_batches_gcs,
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
            filters=RETRIEVAL_FILTERS,
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

    tqdm.pandas()
    linkage_by_chunk = []
    for chunk in tqdm(read_parquet_in_batches_gcs(input_path, batch_size)):
        logger.info(f"chunk length: {len(chunk)} ")
        linkage_candidates_chunk = generate_semantic_candidates(model, chunk)
        linkage_by_chunk.append(linkage_candidates_chunk)
    linkage_candidates = pd.concat(linkage_by_chunk)
    upload_parquet(dataframe=linkage_candidates, gcs_path=f"{output_path}/data.parquet")


if __name__ == "__main__":
    app()
