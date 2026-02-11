import logging
import unicodedata

import pandas as pd
import typer
from sentence_transformers import SentenceTransformer

from src.constants import (
    ENCODER_NAME,
    HF_TOKEN_SECRET_NAME,
    OFFER_NAME_KEY,
    PRODUCT_ID_KEY,
)
from src.utils.deduplication import get_namesakes
from src.utils.gcp import get_secret

PROMPT_NAME = "STS"
BATCH_SIZE = 256

logging.basicConfig(level=logging.INFO)
app = typer.Typer()


def preprocess_offer_name(offer_name: str) -> str:
    normalized = unicodedata.normalize("NFD", offer_name)
    without_accents = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )
    return " ".join(without_accents.lower().strip().split())


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
    HF_TOKEN = get_secret(HF_TOKEN_SECRET_NAME)
    encoder = SentenceTransformer(ENCODER_NAME, token=HF_TOKEN)
    embedding_array = encoder.encode(
        products_of_namesake_artists_df.dropna(
            subset=[OFFER_NAME_KEY]
        ).offer_name.tolist(),
        prompt_name=PROMPT_NAME,
        batch_size=BATCH_SIZE,
        show_progress_bar=True,
    )

    # 3. Save results
    products_of_namesake_artists_df.assign(
        embedding=lambda df: list(embedding_array)
    ).to_parquet(
        output_product_embeddings_filepath,
        index=False,
    )


if __name__ == "__main__":
    app()
