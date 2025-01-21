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

app = typer.Typer()


def filter_candidates(
    linkage_type: str,
    data: pd.DataFrame,
    unmatched_elements: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Filter candidates according to the linkage type.
    """
    if linkage_type == "product":
        return data[data.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
    elif linkage_type == "offer":
        data_offer = data[~data.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
        if unmatched_elements is not None:
            unmatched_candidates = data.merge(
                unmatched_elements[["item_id"]], on="item_id", how="inner"
            )
            return pd.concat([data_offer, unmatched_candidates], ignore_index=True)
        return data_offer

    return data


def process_in_batches(
    label: str,
    input_path: str,
    output_path: str,
    linkage_type: str,
    batch_size: int,
    unmatched_elements: Optional[pd.DataFrame] = None,
):
    """
    Generic routine to read Parquet in batches from GCS, filter, and upload.
    """
    for i, chunk in enumerate(read_parquet_in_batches_gcs(input_path, batch_size)):
        logger.info(f"{label} - raw count: {len(chunk)} items")

        chunk_ready = filter_candidates(linkage_type, chunk, unmatched_elements)
        logger.info(f"{label} - post-filter count: {len(chunk_ready)} items")

        chunk_output_path = f"{output_path}/data-{i + 1}.parquet"
        logger.info(f"{label} - uploading to {chunk_output_path} ...")
        upload_parquet(dataframe=chunk_ready, gcs_path=chunk_output_path)


@app.command()
def main(
    linkage_type: str = typer.Option("true", help="Type of linkage to perform"),
    input_candidates_path: str = typer.Option(..., help="Path to the input catalog"),
    output_candidates_path: str = typer.Option(
        ..., help="Path to save the processed catalog"
    ),
    input_sources_path: Optional[str] = typer.Option(
        None, help="Path to the input catalog"
    ),
    output_sources_path: Optional[str] = typer.Option(
        None, help="Path to save the processed catalog"
    ),
    batch_size: int = typer.Option(
        PARQUET_BATCH_SIZE, help="Batch size for reading the parquet file"
    ),
    unmatched_elements_path: Optional[str] = typer.Option(
        None, help="Unmatched elements"
    ),
):
    """
    Prepare tables for the specified linkage type by reading input data,
    processing offers and sources, and optionally merging unmatched elements.

    This function:
      1) Reads the necessary Parquet data from the given input paths.
      2) Processes or transforms the data as needed for the specified linkage type.
      3) Saves or returns the resulting tables in the specified output paths.
      4) Optionally handles unmatched elements if provided.

    Args:
        linkage_type (str): The type of linkage to prepare tables for.
        input_offers_path (str, optional): Path to the input offers catalog.
        output_offers_path (str, optional): Path where processed offers are saved.
        input_sources_path (str, optional): Path to the input catalog.
        output_sources_path (str, optional): Path to save the processed catalog.
        batch_size (int): The number of rows to read per batch from Parquet.
        unmatched_elements_path (str, optional): Path to any unmatched elements to be handled.

    """
    logger.info(f"Preparing tables for {linkage_type} linkage...")

    if unmatched_elements_path:
        unmatched_elements = read_parquet_files_from_gcs_directory(
            unmatched_elements_path, columns=["item_id"]
        )
    else:
        unmatched_elements = None

    logger.info("Processing candidates...")
    process_in_batches(
        label="Candidates",
        input_path=input_candidates_path,
        output_path=output_candidates_path,
        linkage_type=linkage_type,
        batch_size=batch_size,
        unmatched_elements=unmatched_elements,
    )

    if input_sources_path and output_sources_path:
        logger.info("Processing sources...")
        process_in_batches(
            label="Sources",
            input_path=input_sources_path,
            output_path=output_sources_path,
            linkage_type=linkage_type,
            batch_size=batch_size,
            unmatched_elements=None,
        )


if __name__ == "__main__":
    app()
