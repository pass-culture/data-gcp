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
ITEM_SUFFIX = "_item"
RANK_SEMANTIC_KEY = f"{RANK_KEY}{SEMANTIC_SUFFIX}"
RANK_ITEM_KEY = f"{RANK_KEY}{ITEM_SUFFIX}"
COMBINED_SCORE_KEY = "combined_score"

# LanceDB parameters
LANCEDB_TABLE_NAME = "artist"
LANCEDB_PATH = "data/lancedb"
SEARCH_METRIC = "cosine"
NUM_PARTITIONS = 128

# Reciprocal rank parameters
TT_COEFFICIENT = 0.5
RANK_ALPHA_CONSTANT = 50


def perform_search(
    artist_table: lancedb.Table, selected_artist: dict, similarity_retrieval_method: str
) -> pd.DataFrame:
    return (
        artist_table.search(
            selected_artist[similarity_retrieval_method],
            vector_column_name=similarity_retrieval_method,
        )
        .distance_type("cosine")
        .limit(200)
        .to_pandas()
        .sort_values(
            by=["_distance"],
            ascending=[True],
        )
        .reset_index(drop=True)
        .assign(rank=lambda df: df.index + 1)
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
    result_list = []
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

        results_df = (
            semantic_df.merge(
                item_df,
                on=[
                    ARTIST_ID_KEY,
                    ARTIST_NAME_KEY,
                    ARTIST_APP_SEARCH_SCORE_KEY,
                    ARTIST_BIOGRAPHY_KEY,
                ],
                suffixes=(SEMANTIC_SUFFIX, ITEM_SUFFIX),
                how="outer",
            )
            .loc[
                :,
                [
                    ARTIST_ID_KEY,
                    ARTIST_NAME_KEY,
                    RANK_SEMANTIC_KEY,
                    RANK_ITEM_KEY,
                ],
            ]
            .assign(
                semantic_rank_score=lambda df: (
                    1.0 / (RANK_ALPHA_CONSTANT + df[RANK_SEMANTIC_KEY])
                )
                .infer_objects(copy=False)
                .fillna(0),
                item_rank_score=lambda df: (
                    1.0 / (RANK_ALPHA_CONSTANT + df[RANK_ITEM_KEY])
                )
                .infer_objects(copy=False)
                .fillna(0),
                combined_score=lambda df: df.semantic_rank_score
                + TT_COEFFICIENT * df.item_rank_score,
            )
            .sort_values(by=[COMBINED_SCORE_KEY], ascending=[False])
        )

        result_list.append(
            {
                ARTIST_ID_KEY: selected_artist_row[ARTIST_ID_KEY],
                ARTIST_NAME_KEY: selected_artist_row[ARTIST_NAME_KEY],
                "top_matches": results_df[
                    results_df[ARTIST_ID_KEY] != selected_artist_row[ARTIST_ID_KEY]
                ]  # Exclude the artist itself
                .head(10)
                .to_json(orient="records"),
            }
        )
    logger.info("Similarity search completed. Saving results to Parquet...")

    pd.DataFrame(result_list).to_parquet(output_file_path, index=False)
    logger.info("Results saved to Parquet successfully.")


if __name__ == "__main__":
    app()
