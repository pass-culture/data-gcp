import logging
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

logging.basicConfig(level=logging.INFO)
app = typer.Typer()

BATCH_SIZE = 50
SECTIONS_TO_REMOVE = [
    "References",
    "External links",
    "See also",
    "Notes",
    "Further reading",
]


def fetch_clean_content(wikipedia_titles: list[str], wikipedia_language: str):
    base_url = f"https://{wikipedia_language}.wikipedia.org/w/api.php"

    results = {}

    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "titles": "|".join(wikipedia_titles),
        "redirects": 1,
    }

    try:
        response = requests.post(
            base_url, headers=WIKIMEDIA_REQUEST_HEADER, data=params
        )
        data = response.json()
        pages = data.get("query", {}).get("pages", {})

        for _, page_data in pages.items():
            title = page_data.get("title").replace(" ", "_")
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

                results[title] = clean_text

    except Exception as e:
        print(f"Error: {e}")

    return results


def extract_wikipedia_content_from_url(
    df: pd.DataFrame,
) -> pd.DataFrame:
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
        .groupby("language")
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
        ["language", "batch_index"]
    ):
        t0 = time.time()
        logger.info(f"Processing language: {language}, batch: {batch_index}...")
        wikipedia_pages = group["page_title"].to_list()
        content_dict = fetch_clean_content(
            wikipedia_titles=wikipedia_pages, wikipedia_language=language
        )
        content_df = pd.DataFrame(
            {
                "page_title": list(content_dict.keys()),
                WIKIPEDIA_CONTENT_KEY: list(content_dict.values()),
                "batch_index": batch_index,
                "language": language,
            }
        ).merge(
            group[["page_title", ARTIST_ID_KEY]],
            on="page_title",
        )
        results_df_list.append(content_df)
        logger.success(
            f"...Fetched {len(content_dict)} pages for language: {language}, batch: {batch_index} in {time.time() - t0:.2f} seconds."
        )

    artists_with_content_df = artists_with_wikipedia_url_df.merge(
        pd.concat(results_df_list, ignore_index=True),
        on=[ARTIST_ID_KEY, "page_title", "language", "batch_index"],
        how="left",
    ).loc[:, [*artists_df.columns, WIKIPEDIA_CONTENT_KEY]]
    artists_with_content_df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
