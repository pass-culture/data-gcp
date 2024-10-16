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
WIKI_ID_TO_STRIP = "http://www.wikidata.org/entity/"

app = typer.Typer()


def postprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        wiki_id=lambda df: df.wiki_id.str.strip(WIKI_ID_TO_STRIP),
    )


def merge_data(
    df_list: list[pd.DataFrame], wiki_ids_per_query: dict[str, np.ndarray]
) -> pd.DataFrame:
    # The drop duplicates is done on the wiki_id column due to the fact that professions or aliases can be unsorted lists
    merged_df = pd.concat(df_list).drop_duplicates(subset=["wiki_id"])

    for query_name, wiki_ids in wiki_ids_per_query.items():
        merged_df.loc[lambda df: df.wiki_id.isin(wiki_ids), query_name] = True

    return merged_df


def fetch_wikidata_qlever_csv(sparql_query):
    headers = {"Accept": "text/csv"}

    response = requests.get(
        QLEVER_ENDPOINT, params={"query": sparql_query}, headers=headers
    )

    if response.status_code == 200:
        response.encoding = "utf-8"

        csv_content = response.text
        return pd.read_csv(StringIO(csv_content))
    else:
        print(f"Error: {response.status_code}, {response.text}")


@app.command()
def main(gcs_output_file_path: str = typer.Option()) -> None:
    df_list = []
    wiki_ids_per_query = {}
    for query_name, query_content in QUERIES_PATHES.items():
        logger.info(f"Fetch the data in CSV format for {query_name}")

        with open(query_content, "r") as file:
            query_string = file.read()
        logger.debug(f"SPARQL Query: \n{query_string}")

        data_df = fetch_wikidata_qlever_csv(query_string).pipe(postprocess_data)

        if not data_df.empty:
            print(f"Retrieved {len(data_df)} rows.")
            df_list.append(data_df)
            wiki_ids_per_query[query_name] = data_df.wiki_id.unique()
        else:
            raise ValueError("No data retrieved.")

    logger.info("Merging the data")
    merged_df = merge_data(df_list, wiki_ids_per_query)

    logger.info(f"Saving results to {gcs_output_file_path}")
    merged_df.to_parquet(gcs_output_file_path, index=False)
    logger.info(f"Results saved successfully to {gcs_output_file_path}")


if __name__ == "__main__":
    app()
