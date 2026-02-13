import asyncio
import uuid

import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

from constants import (
    BATCH_SIZE_RETRIEVAL,
    MODEL_PATH,
    NUM_RESULTS,
    RETRIEVAL_FILTERS,
    SEMAPHORE_RETRIEVAL,
)
from model.semantic_space import SemanticSpace
from utils.common import (
    read_parquet_in_batches_gcs,
)
from utils.gcs_utils import upload_parquet

app = typer.Typer()


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
    # Model must be created inside async context (handled in main)
    model = None
    logger.info("Model loaded.")
    return model


def build_filter_dict(row: pd.Series, filter: list) -> dict:
    """
    Build filters for the given row.

    Args:
        row (pd.Series): The row to build filters for.

    Returns:
        dict: The filters.
    """
    filters = {}
    for f in filter:
        filters[f] = getattr(row, f)
    return filters


async def limited_search(model, vector, filters, n):
    """
    Wrapper around model.search with no global semaphore.
    Concurrency is controlled inside SemanticSpace via SEMAPHORE_RETRIEVAL.
    """
    return await model.search(
        vector=vector,
        filters=filters,
        n=n,
    )


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
    
    # We use a queue to distribute work to a fixed number of workers
    queue = asyncio.Queue()
    for row in data.itertuples():
        queue.put_nowait(row)

    async def worker():
        worker_results = []
        while not queue.empty():
            try:
                row = await queue.get()
                # Now we only create the search coroutine when a worker is free
                result_df = await model.search(
                    vector=row.vector,
                    filters=build_filter_dict(row, RETRIEVAL_FILTERS),
                    n=NUM_RESULTS,
                )
                result_df = result_df.assign(
                    candidates_id=str(uuid.uuid4()),
                    item_id_candidate=row.item_id,
                )
                worker_results.append(result_df)
            finally:
                queue.task_done()
        return worker_results

    # Start SEMAPHORE_RETRIEVAL concurrent workers to process the queue
    logger.info(f"Starting {SEMAPHORE_RETRIEVAL} concurrent workers...")
    workers = [worker() for _ in range(SEMAPHORE_RETRIEVAL)]
    
    # Gather results from all workers
    lists_of_results = await asyncio.gather(*workers)
    
    # Flatten results
    for res_list in lists_of_results:
        linkage.extend(res_list)
    
    logger.info(f"linkage length: {len(linkage)}")
    return pd.concat(linkage, ignore_index=True)


@app.command()
def main(
    batch_size: int = typer.Option(default=..., help="Batch size"),
    linkage_type: str = typer.Option(default=..., help="Linkage type"),
    input_path: str = typer.Option(default=..., help="Input table path"),
    output_path: str = typer.Option(default=..., help="Output table path"),
) -> None:
    """
    Generate semantic candidates for linkage,

    This function:
      1) Loads a semantic model based on the specified linkage type.
      2) Reads the input data in chunks of size batch_size.
      3) Generates semantic candidates using the model.
      4) Saves or uploads the resulting data to the specified output path.

    Args:
        batch_size (int): The size of each batch for reading the input data.
        linkage_type (str): The type of linkage (e.g., for different model behaviors).
        input_path (str): The path to the table or file containing input data.
        output_path (str): The path where the output table will be saved.
    """

    tqdm.pandas()
    linkage_by_chunk = []

    async def process_chunks():

        # Create model INSIDE async context (correct event loop lifecycle)
        logger.info("Loading model...")
        model = await SemanticSpace.create(
            MODEL_PATH,
            linkage_type,
            max_concurrency=SEMAPHORE_RETRIEVAL,
        )
        logger.info("Model loaded.")

        for chunk in tqdm(read_parquet_in_batches_gcs(input_path, batch_size)):
            logger.info(f"chunk length: {len(chunk)} ")

            linkage_candidates_chunk = await generate_semantic_candidates(
                model, chunk
            )

            logger.info(
                f"linkage_candidates_chunk length: {len(linkage_candidates_chunk)} "
            )

            linkage_by_chunk.append(linkage_candidates_chunk)
            logger.info(f"linkage_by_chunk length: {len(linkage_by_chunk)} ")

    asyncio.run(process_chunks())

    linkage_candidates = pd.concat(linkage_by_chunk, ignore_index=True)

    logger.info(f"linkage_candidates length: {len(linkage_candidates)} ")
    logger.info("Uploading linkage output..")

    upload_parquet(
        dataframe=linkage_candidates,
        gcs_path=f"{output_path}/data.parquet",
    )


if __name__ == "__main__":
    app()
