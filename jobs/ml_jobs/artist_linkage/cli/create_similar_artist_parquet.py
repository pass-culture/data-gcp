import lancedb
import numpy as np
import pandas as pd
import tqdm
import typer
from loguru import logger

from src.constants import (
    ARTIST_APP_SEARCH_SCORE_KEY,
    ARTIST_BIOGRAPHY_KEY,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    MEAN_TT_ITEM_EMBEDDING_KEY,
)

# Columns
RANK_KEY = "rank"
SEMANTIC_SUFFIX = "_semantic"
ITEM_TT_SUFFIX = "_item_tt"  # Two Tower item-based embedding search suffix
RANK_SEMANTIC_KEY = f"{RANK_KEY}{SEMANTIC_SUFFIX}"
RANK_ITEM_KEY = f"{RANK_KEY}{ITEM_TT_SUFFIX}"
RAW_COMBINED_SCORE_KEY = "raw_combined_score"
COMBINED_SCORE_KEY = "combined_score"

# LanceDB parameters
LANCEDB_TABLE_NAME = "artist"
LANCEDB_PATH = "data/lancedb"
SEARCH_METRIC = "cosine"
NUM_PARTITIONS = 128
MAX_SEARCH_RESULTS = 200

# Reciprocal rank parameters
TT_COEFFICIENT = 0.5
RANK_ALPHA_CONSTANT = 50


def perform_search(
    artist_table: lancedb.Table, selected_artist: dict, similarity_retrieval_method: str
) -> pd.DataFrame:
    """Search for similar artists in a LanceDB table using a specified embedding column.

    Args:
        artist_table: The LanceDB table containing artist embeddings.
        selected_artist: A dict-like row representing the query artist, containing
            the embedding vector under the key specified by `similarity_retrieval_method`.
        similarity_retrieval_method: The name of the vector column to search against
            (e.g. "semantic_embedding" or "mean_tt_item_embedding").

    Returns:
        A DataFrame of the top MAX_SEARCH_RESULTS similar artists, sorted by
        ascending cosine distance, with a zero-based `rank` column added.
    """
    return (
        artist_table.search(
            selected_artist[similarity_retrieval_method],
            vector_column_name=similarity_retrieval_method,
        )
        .distance_type("cosine")
        .limit(MAX_SEARCH_RESULTS)
        .to_pandas()
        .sort_values(
            by=["_distance"],
            ascending=[True],
        )
        .reset_index(drop=True)
        .assign(rank=lambda df: df.index)
    )


def merge_search_results(
    semantic_df: pd.DataFrame, item_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge semantic and item-based search results using Reciprocal Rank Fusion (RRF).

    Combines two ranked lists of artists (from semantic embedding search and
    item two-tower embedding search) into a single ranked list using a weighted
    reciprocal rank scoring approach.

    Args:
        semantic_df: Search results from semantic embedding similarity, containing
            at least ARTIST_ID_KEY, ARTIST_NAME_KEY, and RANK_KEY columns.
        item_df: Search results from item two-tower embedding similarity, containing
            at least ARTIST_ID_KEY and RANK_KEY columns.

    Returns:
        A DataFrame with merged results scored by a combined reciprocal rank formula.
        Includes columns for semantic and item rank scores, raw combined score,
        and a normalized combined score.
    """
    return (
        semantic_df.loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY, RANK_KEY]]
        .merge(
            item_df.loc[:, [ARTIST_ID_KEY, RANK_KEY]],
            on=ARTIST_ID_KEY,
            suffixes=(SEMANTIC_SUFFIX, ITEM_TT_SUFFIX),
            how="outer",
            validate="one_to_one",
        )
        .assign(
            semantic_rank_score=lambda df: (
                1.0 / (RANK_ALPHA_CONSTANT + df[RANK_SEMANTIC_KEY])
            )
            .infer_objects(copy=False)
            .fillna(0),
            item_rank_score=lambda df: (1.0 / (RANK_ALPHA_CONSTANT + df[RANK_ITEM_KEY]))
            .infer_objects(copy=False)
            .fillna(0),
            raw_combined_score=lambda df: df.semantic_rank_score
            + TT_COEFFICIENT * df.item_rank_score,
            combined_score=lambda df: df[RAW_COMBINED_SCORE_KEY]
            / ((1 + TT_COEFFICIENT) / RANK_ALPHA_CONSTANT),
        )
    )


def format_results_df(
    results_df: pd.DataFrame, selected_artist_id: str, selected_artist_name: str
) -> pd.DataFrame:
    """Format and filter merged search results for a given query artist.

    Removes the query artist from results, keeps the top 10 matches by combined
    score, renames artist ID and name columns to indicate they are matches, and
    adds the query artist's ID, name, and a 1-based combined rank column.

    Args:
        results_df: A DataFrame of merged search results, as returned by
            `merge_search_results`, containing at least ARTIST_ID_KEY,
            ARTIST_NAME_KEY, and COMBINED_SCORE_KEY columns.
        selected_artist_id: The ID of the query artist to exclude from results.
        selected_artist_name: The name of the query artist to attach to each row.

    Returns:
        A DataFrame of up to 10 similar artists with renamed match columns,
        the query artist's ID and name, and a combined rank index.
    """
    return (
        results_df.loc[
            lambda df: df[ARTIST_ID_KEY] != selected_artist_id,
            :,
        ]
        .sort_values(by=[COMBINED_SCORE_KEY], ascending=[False])
        .head(10)
        .reset_index(drop=True)
        .rename(
            columns={
                ARTIST_ID_KEY: f"{ARTIST_ID_KEY}_match",
                ARTIST_NAME_KEY: f"{ARTIST_NAME_KEY}_match",
            }
        )
        .assign(
            **{
                ARTIST_ID_KEY: selected_artist_id,
                ARTIST_NAME_KEY: selected_artist_name,
                f"{RANK_KEY}_combined": lambda df: df.index + 1,
            }
        )
    )


app = typer.Typer()


@app.command()
def main(
    artist_with_embeddings_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artist_df = pd.read_parquet(artist_with_embeddings_file_path).assign(
        mean_tt_item_embedding=lambda df: df[MEAN_TT_ITEM_EMBEDDING_KEY].apply(
            lambda x: x if isinstance(x, list | np.ndarray) and len(x) > 0 else None
        )
    )

    # Create lance tables
    logger.info("Creating LanceDB table and indexes...")
    db = lancedb.connect(LANCEDB_PATH)
    if LANCEDB_TABLE_NAME in db.list_tables().tables:
        db.drop_table(LANCEDB_TABLE_NAME)
    artist_table = db.create_table(LANCEDB_TABLE_NAME, artist_df)
    logger.info("LanceDB table created successfully.")

    # Create indexes for both embedding columns to speed up search
    logger.info("Creating indexes for semantic and item embeddings...")
    artist_table.create_index(
        vector_column_name="semantic_embedding",
        metric=SEARCH_METRIC,
        num_partitions=NUM_PARTITIONS,
    )
    artist_table.create_index(
        vector_column_name="mean_tt_item_embedding",
        metric=SEARCH_METRIC,
        num_partitions=NUM_PARTITIONS,
    )
    logger.info("Indexes created successfully.")

    # Perform search for each artist and combine results
    result_df_list = []
    logger.info("Performing similarity search for each artist...")
    for _, selected_artist_row in tqdm.tqdm(artist_df.iterrows(), total=len(artist_df)):
        semantic_df = perform_search(
            artist_table, selected_artist_row, "semantic_embedding"
        )
        if selected_artist_row["mean_tt_item_embedding"] is not None:
            item_df = perform_search(
                artist_table, selected_artist_row, "mean_tt_item_embedding"
            )
        else:
            item_df = pd.DataFrame(
                columns=[
                    ARTIST_ID_KEY,
                    ARTIST_NAME_KEY,
                    ARTIST_APP_SEARCH_SCORE_KEY,
                    ARTIST_BIOGRAPHY_KEY,
                    RANK_KEY,
                ]
            )

        results_df = merge_search_results(semantic_df=semantic_df, item_df=item_df)
        formatted_results_df = format_results_df(
            results_df=results_df,
            selected_artist_id=selected_artist_row[ARTIST_ID_KEY],
            selected_artist_name=selected_artist_row[ARTIST_NAME_KEY],
        )
        result_df_list.append(formatted_results_df)

    if len(result_df_list) == 0:
        raise Exception("No results found for any artist. Exiting without saving.")

    logger.info("Similarity search completed. Saving results to Parquet...")
    pd.concat(result_df_list).loc[:, lambda df: df.columns.sort_values()].to_parquet(
        output_file_path, index=False
    )
    logger.info("Results saved to Parquet successfully.")


if __name__ == "__main__":
    app()
