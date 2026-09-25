from urllib.parse import unquote

import mwparserfromhell
import pandas as pd
import requests
from loguru import logger

from src.common.constants import WIKIPEDIA_URL_KEY
from src.similarity.constants import (
    ARTIST_BIOGRAPHY_KEY,
    WIKIMEDIA_REQUEST_HEADER,
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
    preprocessed_df = df.assign(
        page_title=lambda df: df[WIKIPEDIA_URL_KEY]
        .str.split("wiki/")
        .str[1]
        .apply(unquote)
        .str.replace(" ", "_"),
        language=lambda df: df[WIKIPEDIA_URL_KEY].str.extract(
            r"https://([a-z]{2})\.wikipedia\.org"
        ),
    )

    if preprocessed_df[WIKIPEDIA_URL_KEY].isna().all():
        logger.warning(
            "All Wikipedia URLs are missing. Please check the input data. Returning original DataFrame with added columns filled with NaN."
        )
        return df.assign(
            page_title=pd.NA,
            language=pd.NA,
            batch_index=pd.NA,
        )

    return (
        preprocessed_df.groupby(LANGUAGE_COLUMN)
        .apply(
            lambda group: group.assign(
                batch_index=lambda df: df.reset_index().index // BATCH_SIZE
            )
        )
        .reset_index(drop=True)
    )


def get_artists_to_extract_wikipedia_content_filter(
    artists_df: pd.DataFrame,
    *,
    extract_all_from_scratch: bool,
) -> pd.Series:
    """ "
    Returns a boolean Series indicating which artists should have their Wikipedia content extracted based on the presence of a Wikipedia URL and existing biography content, depending on the incremental or from-scratch mode.
    Args:
        artists_df: DataFrame containing artist data, including Wikipedia URLs and biographies.
        extract_all_from_scratch: If True, all artists with a Wikipedia URL are included regardless of existing biography content.
    Returns:
        A boolean Series where True indicates the artist should be processed for Wikipedia content extraction.
    """
    if extract_all_from_scratch:
        logger.info(
            "Extracting Wikipedia content for all artists with a Wikipedia URL, regardless of existing biography content."
        )
        filters_series = artists_df[WIKIPEDIA_URL_KEY].notna()
    else:
        logger.info(
            "Extracting Wikipedia content only for artists with a Wikipedia URL and missing biography content."
        )
        filters_series = artists_df[WIKIPEDIA_URL_KEY].notna() & (
            artists_df[ARTIST_BIOGRAPHY_KEY].isna()
            | artists_df[ARTIST_BIOGRAPHY_KEY].eq("")
        )
    logger.info(f"{filters_series.sum()} artists with a Wikipedia URL to process.")
    return filters_series
