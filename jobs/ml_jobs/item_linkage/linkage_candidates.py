import asyncio
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
MAX_CONCURRENT_SEARCHES = 400
search_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEARCHES)


def load_model(model_path: str, linkage_type: str) -> SemanticSpace:
    """
    Load the SemanticSpace model from the given path.

    Args:
        model_path (str): Path to the model.
        reduction (bool): Whether to reduce the embeddings.

    Returns:
        SemanticSpace: The loaded model.
    """
    logger.info("Loading model...")
    model = SemanticSpace(model_path, linkage_type)
    logger.info("Model loaded.")
    return model


def build_filter_dict(row, filter: list = RETRIEVAL_FILTERS):
    """
    Build filters for the given row.

    Args:
        row (pd.Series): The row to build filters for.

    Returns:
        dict: The filters.
    """
    filters = {}
    for f in filter:
        filters[f] = row[f]
    return filters


# Example semaphore defined outside (adjust permits as needed)
search_semaphore = asyncio.Semaphore(100)
# Tweak this based on your machine and desired parallelism:
BATCH_SIZE = 10000


# -----------------------
# 1) Search function
# -----------------------
async def limited_search(
    model, vector, filters, similarity_metric, n, vector_column_name
):
    async with search_semaphore:
        return await model.search(
            vector=vector,
            filters=filters,
            similarity_metric=similarity_metric,
            n=n,
            vector_column_name=vector_column_name,
        )


# -----------------------
# 2) Main function
# -----------------------
async def generate_semantic_candidates(
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

    # Create all tasks first
    tasks = []
    for index, row in tqdm(
        data.iterrows(), total=data.shape[0], desc="Processing rows", mininterval=60
    ):
        tasks.append(
            limited_search(
                model=model,
                vector=row.vector,
                filters=build_filter_dict(row, RETRIEVAL_FILTERS),
                similarity_metric="cosine",
                n=NUM_RESULTS,
                vector_column_name="vector",
            )
        )

    logger.info("Tasks ready!")
    logger.info(f"Tasks length: {len(tasks)}")

    # Gather results in batches to control concurrency
    results = []
    for i in tqdm(range(0, len(tasks), BATCH_SIZE), desc="Gathering batches"):
        batch = tasks[i : i + BATCH_SIZE]
        # Wait for this batch to complete
        batch_results = await asyncio.gather(*batch)
        results.extend(batch_results)

    logger.info("Finished gathering all batches!")

    # Now, zip your final results with the original rows
    for result_df, row in zip(results, data.itertuples()):
        result_df = result_df.assign(
            candidates_id=str(uuid.uuid4()), item_id_candidate=row.item_id
        )
        linkage.append(result_df)

    logger.info(f"linkage length: {len(linkage)}")
    return pd.concat(linkage)


@app.command()
def main(
    batch_size: int = typer.Option(default=..., help="Batch size"),
    linkage_type: str = typer.Option(default=..., help="Linkage type"),
    input_path: str = typer.Option(default=..., help="Input table path"),
    output_path: str = typer.Option(default=..., help="Output table path"),
) -> None:
    """
    Main function to preprocess data, prepare vectors, generate semantic candidates, and upload the results to GCS.

    Args:
        batch_size (int): The size of each batch.
        linkage_type (str): The type of linkage to perform.
        input_path (str): The path to the input table.
        output_path (str): The path to the output table.
        unmatched_elements_path (str): The path to the unmatched elements table.
    """
    model = load_model(MODEL_PATH, linkage_type)

    tqdm.pandas()
    linkage_by_chunk = []

    async def process_chunks():
        for chunk in tqdm(read_parquet_in_batches_gcs(input_path, batch_size)):
            logger.info(f"chunk length: {len(chunk)} ")
            linkage_candidates_chunk = await generate_semantic_candidates(model, chunk)
            logger.info(
                f"linkage_candidates_chunk length: {len(linkage_candidates_chunk)} "
            )
            linkage_by_chunk.append(linkage_candidates_chunk)
            logger.info(f"linkage_by_chunk length: {len(linkage_by_chunk)} ")

    asyncio.run(process_chunks())
    linkage_candidates = pd.concat(linkage_by_chunk)
    logger.info(f"linkage_candidates length: {len(linkage_candidates)} ")
    logger.info("Uploading linkage output..")
    upload_parquet(dataframe=linkage_candidates, gcs_path=f"{output_path}/data.parquet")


if __name__ == "__main__":
    app()
