import json
from datetime import datetime

import pandas as pd
import typer

from src.constants import TITELIVE_CATEGORIES

app = typer.Typer()

OFFER_CATEGORY_OPTION = typer.Option(..., help="Category of offers to extract")
MIN_MODIFIED_DATE_OPTION = typer.Option(..., help="Minimum modified date for offers")
INPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the input file")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")


BOOK_OUTPUT_COLUMNS_WITH_TYPES = {
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


def post_process_before_saving(df: pd.DataFrame):
    ENFORCE_COLUMN_TYPES = {
        "article_taux_tva": float,
        "article_image": int,
        "article_iad": int,
    }

    # Convert dict like columns to JSON
    for col in df.select_dtypes(include=[object]).columns:
        df[col] = df[col].replace(["None", "nan", "NaN"], None)

        if df[col].dropna().apply(lambda x: isinstance(x, dict | list)).any():
            df[col] = df[col].map(json.dumps).replace(["None", "nan", "NaN"], None)

        if col in ENFORCE_COLUMN_TYPES:
            df[col] = df[col].astype(ENFORCE_COLUMN_TYPES[col])

    return df


@app.command()
def format_products(
    offer_category: TITELIVE_CATEGORIES = OFFER_CATEGORY_OPTION,
    min_modified_date: datetime = MIN_MODIFIED_DATE_OPTION,
    input_file_path: str = INPUT_FILE_PATH_OPTION,
    output_file_path: str = OUTPUT_FILE_PATH_OPTION,
):
    """
    Extract new offers from Titelive API.
    """
    min_formatted_date = min_modified_date.strftime("%d/%m/%Y")

    if offer_category != TITELIVE_CATEGORIES.LIVRE:
        raise NotImplementedError("Formatting is only implemented for books yet.")

    raw_products_df = pd.read_parquet(input_file_path)

    exploded_df = (
        pd.DataFrame(raw_products_df["data"].apply(json.loads).to_list())
        .assign(
            article_list=lambda _df: _df.article.map(lambda o: list(o.values())),
        )
        .explode("article_list")
    )

    merged_df = (
        pd.concat(
            [
                exploded_df.drop(columns=["article_list"]),
                exploded_df["article_list"].apply(pd.Series).add_prefix("article_"),
            ],
            axis=1,
        )
        .assign(
            auteurs_multi=lambda df: df.auteurs_multi.map(json.dumps),
        )
        .drop(columns=["article"])
    )

    # min_modified_date = datetime.strptime(min_modified_date, "%Y-%m-%d")
    filtered_df = merged_df.loc[
        (merged_df["article_datemodification"].isna())
        | (pd.to_datetime(merged_df["article_datemodification"]) >= min_formatted_date)
    ]

    final_df = (
        filtered_df.loc[:, BOOK_OUTPUT_COLUMNS_WITH_TYPES.keys()]
        .astype(BOOK_OUTPUT_COLUMNS_WITH_TYPES)
        .pipe(post_process_before_saving)
    )

    final_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
