import time
from io import StringIO

import numpy as np
import pandas as pd
import requests
import typer
from loguru import logger

QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"
QUERIES_PATHES = {
    "book": "queries/extract_book_artists.rq",
    "movie": "queries/extract_movie_artists.rq",
    "music": "queries/extract_music_artists.rq",
    "gkg": "queries/extract_gkg_artists.rq",
}
QUERIES_PARAMS = {"retries": 6, "delay": 300}

app = typer.Typer()


def extract_wiki_id(df: pd.DataFrame) -> pd.DataFrame:
    WIKI_ID_TO_STRIP = "http://www.wikidata.org/entity/"
    return df.assign(
        wiki_id=lambda df: df.wiki_id.str.strip(WIKI_ID_TO_STRIP),
    )


def merge_data(
    df_list: list[pd.DataFrame], wiki_ids_per_query: dict[str, np.ndarray]
) -> pd.DataFrame:
    # The drop duplicates is done on the wiki_id column due to the fact that professions or aliases can be unsorted lists
    merged_df = pd.concat(df_list).drop_duplicates(subset=["wiki_id"])

    for query_name, wiki_ids in wiki_ids_per_query.items():
        merged_df = merged_df.assign(
            **{query_name: lambda df: df.wiki_id.isin(wiki_ids)}
        )

    return merged_df


def postprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    EMPTY_ALIAS_KEYWORD = "EMPTY_ALIAS"
    SEPARATOR_KEY = "|"  # Wikidata Queries also use the '|' separator, so be careful when changing this
    return (
        df.assign(
            artist_name=lambda df: df.artist_name_fr.combine_first(df.artist_name_en),
            aliases=lambda df: (
                df.artist_name_fr.fillna(EMPTY_ALIAS_KEYWORD)
                + "|"
                + df.artist_name_en.fillna(EMPTY_ALIAS_KEYWORD)
                + "|"
                + df.aliases_fr.fillna(EMPTY_ALIAS_KEYWORD)
                + "|"
                + df.aliases_en.fillna(EMPTY_ALIAS_KEYWORD)
            )
            .str.replace(f"{SEPARATOR_KEY}{EMPTY_ALIAS_KEYWORD}", "")
            .str.replace(f"{EMPTY_ALIAS_KEYWORD}{SEPARATOR_KEY}", ""),
            aliases_list=lambda df: df.aliases.str.split(SEPARATOR_KEY),
        )
        .drop(
            columns=[
                "artist_name_fr",
                "artist_name_en",
                "aliases_fr",
                "aliases_en",
                "aliases",
            ]
        )
        .explode("aliases_list")
        .rename(columns={"aliases_list": "alias"})
        .loc[lambda df: df.alias != EMPTY_ALIAS_KEYWORD]
        .drop_duplicates()
    )


def fetch_wikidata_qlever_csv(sparql_query, retries=3, delay=5):
    headers = {"Accept": "text/csv"}

    for attempt in range(QUERIES_PARAMS["retries"]):
        try:
            response = requests.get(
                QLEVER_ENDPOINT, params={"query": sparql_query}, headers=headers
            )

            if response.status_code == 200:
                response.encoding = "utf-8"
                csv_content = response.text
                return pd.read_csv(StringIO(csv_content))
            else:
                print(
                    f"Attempt {attempt + 1} - Error: {response.status_code}, {response.text}"
                )

        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} - Request error: {e}")

        # Wait before retrying if not the last attempt
        if attempt < QUERIES_PARAMS["retries"] - 1:
            time.sleep(QUERIES_PARAMS["delay"])

    # Return None if all attempts failed
    print("Failed to retrieve data after multiple attempts.")
    return None


@app.command()
def main(output_file_path: str = typer.Option()) -> None:
    df_list = []
    wiki_ids_per_query = {}
    for query_name, query_content in QUERIES_PATHES.items():
        logger.info(f"Fetch the data in CSV format for {query_name}")

        with open(query_content, "r") as file:
            query_string = file.read()
        logger.debug(f"SPARQL Query: \n{query_string}")

        data_df = fetch_wikidata_qlever_csv(query_string).pipe(extract_wiki_id)

        if not data_df.empty:
            print(f"Retrieved {len(data_df)} rows.")
            df_list.append(data_df)
            wiki_ids_per_query[query_name] = data_df.wiki_id.unique()
        else:
            raise ValueError("No data retrieved.")

    logger.info("Merging the data")
    merged_df = merge_data(df_list, wiki_ids_per_query)

    logger.info("Postprocessing the data")
    postprocessed_df = postprocess_data(merged_df)
    logger.info(
        f"Found {len(postprocessed_df)} unique (wiki_id, alias) pairs for {postprocessed_df.wiki_id.nunique()} wiki_ids"
    )

    logger.info(f"Saving results to {output_file_path}")
    postprocessed_df.to_parquet(output_file_path, index=False)
    logger.info(f"Results saved successfully to {output_file_path}")


if __name__ == "__main__":
    app()
