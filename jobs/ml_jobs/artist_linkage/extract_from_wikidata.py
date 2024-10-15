from io import StringIO

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
    for query_name, query_content in QUERIES_PATHES.items():
        logger.info(f"Fetch the data in CSV format for {query_name}")

        with open(query_content, "r") as file:
            query_string = file.read()
        logger.debug(f"SPARQL Query: \n{query_string}")

        data_df = fetch_wikidata_qlever_csv(query_string).assign(
            query_name=query_name,
            wiki_id=lambda df: df.wiki_id.str.strip(WIKI_ID_TO_STRIP),
        )
        if not data_df.empty:
            print(f"Retrieved {len(data_df)} rows.")

            df_list.append(data_df)
        else:
            raise ValueError("No data retrieved.")

    logger.info(f"Saving results to {gcs_output_file_path}")
    pd.concat(df_list).to_parquet(gcs_output_file_path, index=False)
    logger.info(f"Results saved successfully to {gcs_output_file_path}")


if __name__ == "__main__":
    app()
