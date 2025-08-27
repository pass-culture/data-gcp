from datetime import datetime

import typer

from src.constants import TITELIVE_CATEGORIES
from src.utils.requests import get_modified_offers

app = typer.Typer()

OFFER_CATEGORY_OPTION = typer.Option(..., help="Category of offers to extract")
MIN_MODIFIED_DATE_OPTION = typer.Option(..., help="Minimum modified date for offers")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")


@app.command()
def extract_new_books(
    offer_category: TITELIVE_CATEGORIES = OFFER_CATEGORY_OPTION,
    min_modified_date: datetime = MIN_MODIFIED_DATE_OPTION,
    output_file_path: str = OUTPUT_FILE_PATH_OPTION,
):
    """
    Extract new offers from Titelive API.
    """
    get_modified_offers(offer_category, min_modified_date).to_parquet(output_file_path)


if __name__ == "__main__":
    app()
