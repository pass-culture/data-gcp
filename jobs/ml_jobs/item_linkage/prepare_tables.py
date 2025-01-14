from typing import Optional

import typer
from loguru import logger

# Constants
from constants import SYNCHRO_SUBCATEGORIES
from utils.common import read_parquet_files_from_gcs_directory
from utils.gcs_utils import upload_parquet

# Typer app instance
app = typer.Typer()


def filter_sources_candidates(
    linkage_type, unmatched_elements_path, sources, candidates
):
    if linkage_type == "product":
        sources = sources[sources.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)]
        candidates = candidates[
            candidates.offer_subcategory_id.isin(SYNCHRO_SUBCATEGORIES)
        ]
    elif linkage_type == "offer":
        unmatched_elements = read_parquet_files_from_gcs_directory(
            unmatched_elements_path,
            columns=["item_id"],
        )
        sources = sources.merge(
            unmatched_elements[["item_id"]], on="item_id", how="inner"
        )
        candidates = candidates.merge(
            unmatched_elements[["item_id"]], on="item_id", how="inner"
        )

    return sources, candidates


# Main Typer Command
@app.command()
def main(
    linkage_type: str = typer.Option(
        default="true",
        help="Type of linkage to perform",
    ),
    input_sources_path: str = typer.Option(..., help="Path to the input catalog"),
    input_candidates_path: str = typer.Option(..., help="Path to the input catalog"),
    output_sources_path: str = typer.Option(
        ..., help="Path to save the processed catalog"
    ),
    output_candidates_path: str = typer.Option(
        ..., help="Path to save the processed catalog"
    ),
    unmatched_elements_path: Optional[str] = typer.Option(
        default=None, help="Unmatched elements"
    ),
):
    """
    Main function to preprocess catalog data.
    """
    logger.info(f"Preparing tables for {linkage_type} linkage...")
    sources = read_parquet_files_from_gcs_directory(
        input_sources_path,
        columns=["item_id"],
    )
    candidates = read_parquet_files_from_gcs_directory(
        input_candidates_path,
        columns=["item_id"],
    )
    logger.info(f"Sources clean: {len(sources)} items")
    logger.info(f"Candidates clean: {len(candidates)} items")
    sources, candidates = filter_sources_candidates(
        linkage_type, unmatched_elements_path, sources, candidates
    )
    logger.info(f"Sources ready: {len(sources)} items")
    logger.info(f"Candidates ready: {len(candidates)} items")
    upload_parquet(
        dataframe=sources,
        gcs_path=output_sources_path,
    )
    upload_parquet(
        dataframe=candidates,
        gcs_path=output_candidates_path,
    )


if __name__ == "__main__":
    app()
