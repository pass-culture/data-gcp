from typing import Optional

import pandas as pd
import typer
from loguru import logger

# Constants
from constants import PARQUET_BATCH_SIZE, SYNCHRO_SUBCATEGORIES
from utils.common import (
    read_parquet_files_from_gcs_directory,
    read_parquet_in_batches_gcs,
)
from utils.gcs_utils import upload_parquet

# Typer app instance
app = typer.Typer()


def filter_candidates(linkage_type, data, unmatched_elements=None):
    if linkage_type == "product":
        data_ready = data[data.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
    elif linkage_type == "offer":
        data_offer = data[~data.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]

        unmatched_candidates = data.merge(
            unmatched_elements[["item_id"]], on="item_id", how="inner"
        )
        data_ready = pd.concat([data_offer, unmatched_candidates])

    return data_ready


# Main Typer Command
@app.command()
def main(
    linkage_type: str = typer.Option(
        default="true",
        help="Type of linkage to perform",
    ),
    input_candidates_path: str = typer.Option(..., help="Path to the input catalog"),
    output_candidates_path: str = typer.Option(
        ..., help="Path to save the processed catalog"
    ),
    input_sources_path: Optional[str] = typer.Option(
        default=None, help="Path to the input catalog"
    ),
    output_sources_path: Optional[str] = typer.Option(
        default=None, help="Path to save the processed catalog"
    ),
    batch_size: int = typer.Option(
        default=PARQUET_BATCH_SIZE,
        help="Batch size for reading the parquet file",
    ),
    unmatched_elements_path: Optional[str] = typer.Option(
        default=None, help="Unmatched elements"
    ),
):
    """
    Main function to preprocess catalog data.
    """
    logger.info(f"Preparing tables for {linkage_type} linkage...")
    if unmatched_elements_path:
        unmatched_elements = read_parquet_files_from_gcs_directory(
            unmatched_elements_path,
            columns=["item_id"],
        )
    else:
        unmatched_elements = None

    for i, chunk in enumerate(
        read_parquet_in_batches_gcs(input_candidates_path, batch_size)
    ):
        logger.info(f"Candidates clean: {len(chunk)} items")
        chunk_ready = filter_candidates(linkage_type, chunk, unmatched_elements)
        logger.info(f"Candidates ready: {len(chunk_ready)} items")
        chunk_output_path = f"{output_candidates_path}/data-{i + 1}.parquet"
        logger.info(f"Saving processed chunk to {chunk_output_path}...")
        upload_parquet(
            dataframe=chunk_ready,
            gcs_path=chunk_output_path,
        )
    if output_sources_path:
        for i, chunk in enumerate(
            read_parquet_in_batches_gcs(input_sources_path, batch_size)
        ):
            logger.info(f"Sources clean: {len(chunk)} items")
            chunk_ready = filter_candidates(linkage_type, chunk)
            logger.info(f"Sources ready: {len(chunk_ready)} items")
            chunk_output_path = f"{output_sources_path}/data-{i + 1}.parquet"
            logger.info(f"Saving processed chunk to {chunk_output_path}...")
            upload_parquet(
                dataframe=chunk_ready,
                gcs_path=chunk_output_path,
            )


if __name__ == "__main__":
    app()
