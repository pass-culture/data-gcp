import logging
import os
import unicodedata

import pandas as pd
import typer
from sentence_transformers import SentenceTransformer

from src.constants import OFFER_NAME_KEY, PRODUCT_ID_KEY

ENCODER_NAME = "google/embeddinggemma-300m"
PROMPT_NAME = "STS"
BATCH_SIZE = 256
THRESHOLD = 0.7
RANDOM_SEED = 42

logging.basicConfig(level=logging.INFO)
app = typer.Typer()


def preprocess_offer_name(offer_name: str) -> str:
    normalized = unicodedata.normalize("NFD", offer_name)
    without_accents = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )
    return " ".join(without_accents.lower().strip().split())


def get_namesakes(artist_with_stats_df: pd.DataFrame) -> pd.DataFrame:
    return (
        artist_with_stats_df.groupby("normalized_artist_name")
        .agg(
            artist_id_list=("artist_id", list),
            artist_names=("artist_name", list),
            artist_count=("artist_id", "nunique"),
        )
        .loc[lambda df: df.artist_count > 1]
    )


@app.command()
def main(
    applicative_product_artist_link_filepath: str = typer.Option(),
    artist_score_filepath: str = typer.Option(),
    product_stats_filepath: str = typer.Option(),
    output_product_embeddings_filepath: str = typer.Option(),
) -> None:
    # 1. Load raw data
    artist_df = pd.read_parquet(artist_score_filepath)
    applicative_product_artist_link_df = pd.read_parquet(
        applicative_product_artist_link_filepath
    )
    product_stats_df = pd.read_parquet(product_stats_filepath).dropna(
        subset=[OFFER_NAME_KEY]
    )

    # 2. Preprocess data
    namesake_artist_df = get_namesakes(artist_df)
    products_of_namesake_artists_df = (
        applicative_product_artist_link_df.loc[
            lambda df: df.artist_id.isin(
                namesake_artist_df.explode("artist_id_list").artist_id_list
            )
        ]
        .merge(product_stats_df, on=PRODUCT_ID_KEY, how="inner")
        .assign(
            preprocessed_offer_name=lambda df: df[OFFER_NAME_KEY].map(
                preprocess_offer_name
            )
        )
        .drop_duplicates()
    )

    # 3. Encode offer names
    encoder = SentenceTransformer(ENCODER_NAME, token=os.environ.get("HF_TOKEN"))
    embedding_array = encoder.encode(
        products_of_namesake_artists_df.drop_duplicates()
        .dropna(subset=[OFFER_NAME_KEY])
        .offer_name.tolist(),
        prompt_name=PROMPT_NAME,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
    )

    # 3. Save results
    products_of_namesake_artists_df.assign(
        embeddings=lambda df: list(embedding_array)
    ).to_parquet(
        output_product_embeddings_filepath,
        index=False,
    )


if __name__ == "__main__":
    app()
