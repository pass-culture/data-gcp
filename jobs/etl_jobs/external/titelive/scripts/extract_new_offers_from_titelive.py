from datetime import datetime

import typer

from src.constants import TITELIVE_CATEGORIES
from src.utils.requests import get_modified_offers

app = typer.Typer()

OFFER_CATEGORY_OPTION = typer.Option(..., help="Category of offers to extract")
MIN_MODIFIED_DATE_OPTION = typer.Option(..., help="Minimum modified date for offers")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")


OUTPUT_COLUMNS_WITH_TYPES = {
    "titre": str,
    "auteurs_multi": str,
    "article_langueiso": str,
    "article_taux_tva": float,
    "article_id_lectorat": float,
    "article_resume": str,
    "article_gencod": str,
    "article_codesupport": str,
    "article_gtl": str,
    "article_dateparution": str,
    "article_editeur": str,
    "article_prix": float,
}


@app.command()
def extract_titelive_products(
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
