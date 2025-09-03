from datetime import datetime

import pandas as pd
import typer

from src.constants import TITELIVE_CATEGORIES
from src.utils.requests import get_modified_offers

app = typer.Typer()

OFFER_CATEGORY_OPTION = typer.Option(..., help="Category of offers to extract")
MIN_MODIFIED_DATE_OPTION = typer.Option(..., help="Minimum modified date for offers")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")


def post_process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Post-process the DataFrame by removing duplicate rows.
    Args:
        df (pd.DataFrame): Input DataFrame that may contain duplicate rows.
    Returns:
        pd.DataFrame: DataFrame with duplicate rows removed
    """

    return df.drop_duplicates()


@app.command()
def extract_titelive_products(
    offer_category: TITELIVE_CATEGORIES = OFFER_CATEGORY_OPTION,
    min_modified_date: datetime = MIN_MODIFIED_DATE_OPTION,
    output_file_path: str = OUTPUT_FILE_PATH_OPTION,
):
    """
    Extract new offers from Titelive API.
    """
    get_modified_offers(offer_category, min_modified_date).pipe(
        post_process_data
    ).to_parquet(output_file_path)


if __name__ == "__main__":
    app()
