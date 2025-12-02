import time
from urllib.parse import unquote

import mwparserfromhell
import pandas as pd
import requests
import typer
from loguru import logger

from constants import (
    ARTIST_ID_KEY,
    WIKIMEDIA_REQUEST_HEADER,
    WIKIPEDIA_CONTENT_KEY,
    WIKIPEDIA_URL_KEY,
)

# Wikimedia API settings
BATCH_SIZE = 50
SECTIONS_TO_REMOVE = [
    "References",
    "External links",
    "See also",
    "Notes",
    "Further reading",
]
BASE_QUERY_PARAMS = {
    "action": "query",
    "format": "json",
    "prop": "revisions",
    "rvprop": "content",
    "rvslots": "main",
    "redirects": 1,
}

# Column names for current file
PAGE_TITLE_COLUMN = "page_title"
LANGUAGE_COLUMN = "language"
BATCH_INDEX_COLUMN = "batch_index"

app = typer.Typer()


def fetch_clean_content(
    wikipedia_titles: list[str], wikipedia_language: str
) -> dict[str, str]:
    """
    Fetches and cleans content from Wikipedia pages for a given list of titles and language.

    Args:
        wikipedia_titles: A list of Wikipedia page titles to fetch.
        wikipedia_language: The language code of the Wikipedia edition (e.g., 'en', 'fr').

    Returns:
        A dictionary where keys are page titles and values are the cleaned text content.
    """
    base_url = f"https://{wikipedia_language}.wikipedia.org/w/api.php"
    params = {
        **BASE_QUERY_PARAMS,
        "titles": "|".join(wikipedia_titles),
    }

    results = {}
    try:
        response = requests.post(
            base_url, headers=WIKIMEDIA_REQUEST_HEADER, data=params
        )
        data = response.json()
        pages = data.get("query", {}).get("pages", {})
        redirect_mapping = {
            item["to"]: item["from"]
            for item in data.get("query", {}).get("redirects", [])
        }  # Map redirected titles to original titles in case wikipedia url was redirected

        for _, page_data in pages.items():
            raw_title = redirect_mapping.get(
                page_data.get("title"), page_data.get("title")
            ).replace(" ", "_")
            revisions = page_data.get("revisions", [])

            if revisions:
                raw_wikitext = revisions[0].get("slots", {}).get("main", {}).get("*")

                # --- 1. Parse the Code ---
                wikicode = mwparserfromhell.parse(raw_wikitext)

                # --- 2. Drop irrelevant sections ---
                # We don't want the LLM summarizing the "References" or "See Also" lists
                for section in wikicode.get_sections(
                    matches="|".join(SECTIONS_TO_REMOVE)
                ):
                    wikicode.remove(section)

                # --- 3. Convert to Plain Text ---
                # strip_code() removes '''bold''', [[links]], and {{templates}}
                clean_text = wikicode.strip_code()

                results[raw_title] = clean_text

    except Exception as e:
        logger.error(f"Error: {e}")

    return results


def extract_wikipedia_content_from_url(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extracts metadata (page title, language) from Wikipedia URLs and assigns batch indices.

    Args:
        df: DataFrame containing Wikipedia URLs.

    Returns:
        DataFrame with added columns for page title, language, and batch index.
    """
    return (
        df.assign(
            page_title=lambda df: df[WIKIPEDIA_URL_KEY]
            .str.split("wiki/")
            .str[1]
            .apply(unquote)
            .str.replace(" ", "_"),
            language=lambda df: df[WIKIPEDIA_URL_KEY].str.extract(
                r"https://([a-z]{2})\.wikipedia\.org"
            ),
        )
        .groupby(LANGUAGE_COLUMN)
        .apply(
            lambda group: group.assign(
                batch_index=lambda df: df.reset_index().index // BATCH_SIZE
            )
        )
        .reset_index(drop=True)
    )


@app.command()
def main(
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artists_matched_on_wikidata)

    # Prepare Data
    artists_with_wikipedia_url_df = artists_df.loc[
        lambda df: df[WIKIPEDIA_URL_KEY].notna()
    ].pipe(extract_wikipedia_content_from_url)

    # Fetch the wikipedia page content from MediaWiki API
    results_df_list = []
    for (language, batch_index), group in artists_with_wikipedia_url_df.groupby(
        [LANGUAGE_COLUMN, BATCH_INDEX_COLUMN]
    ):
        t0 = time.time()
        wikipedia_pages = group[PAGE_TITLE_COLUMN].to_list()
        logger.info(f"Processing language: {language}, batch: {batch_index}...")
        content_dict = fetch_clean_content(
            wikipedia_titles=wikipedia_pages, wikipedia_language=language
        )
        content_df = pd.DataFrame(
            {
                PAGE_TITLE_COLUMN: list(content_dict.keys()),
                WIKIPEDIA_CONTENT_KEY: list(content_dict.values()),
                BATCH_INDEX_COLUMN: batch_index,
                LANGUAGE_COLUMN: language,
            }
        ).merge(
            group[[PAGE_TITLE_COLUMN, ARTIST_ID_KEY]],
            on=PAGE_TITLE_COLUMN,
            how="left",
        )
        results_df_list.append(content_df)
        logger.success(
            f"...Fetched {len(content_dict)} pages for language: {language}, batch: {batch_index} in {time.time() - t0:.2f} seconds."
        )

    # Merge back the wikipedia content to the original dataframe
    (
        artists_with_wikipedia_url_df.merge(
            pd.concat(results_df_list, ignore_index=True),
            on=[ARTIST_ID_KEY, PAGE_TITLE_COLUMN, LANGUAGE_COLUMN, BATCH_INDEX_COLUMN],
            how="left",
        )
        .loc[:, [*artists_df.columns, WIKIPEDIA_CONTENT_KEY]]
        .to_parquet(output_file_path, index=False)
    )


if __name__ == "__main__":
    app()
