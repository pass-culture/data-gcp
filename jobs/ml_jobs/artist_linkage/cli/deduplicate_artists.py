import logging

import networkx as nx
import numpy as np
import pandas as pd
import tqdm
import typer
from loguru import logger
from sentence_transformers import SentenceTransformer

from src.constants import (
    ACTION_KEY,
    ARTIST_ALIASES_KEYS,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    COMMENT_KEY,
    ENCODER_NAME,
    HF_TOKEN,
    Action,
)
from src.utils.deduplication import get_namesakes

logging.basicConfig(level=logging.INFO)
app = typer.Typer()

THRESHOLD = 0.7


def get_offer_name_similarities_on_df(
    encoder: SentenceTransformer, offer_names_df: pd.DataFrame
) -> pd.DataFrame:
    try:
        similarities = encoder.similarity(
            offer_names_df.embedding.tolist(), offer_names_df.embedding.tolist()
        ).reshape(-1)
    except:  # noqa: E722
        similarities = np.array(len(offer_names_df) * len(offer_names_df)).reshape(-1)
    offer_without_embeddings_df = offer_names_df.drop(columns=["embedding"])
    return (
        offer_without_embeddings_df.join(
            offer_without_embeddings_df, lsuffix="_1", rsuffix="_2", how="cross"
        )
        .assign(
            has_common_offer_name=lambda df: (
                df.preprocessed_offer_name_1.str.lower().str.strip()
                == df.preprocessed_offer_name_2.str.lower().str.strip()
            ),
            embedding_dot=similarities,
        )
        .loc[lambda df: df.artist_id_1 != df.artist_id_2]
    )


def get_artists_to_merge_for_matched_artists(
    matched_artists_df: pd.DataFrame,
) -> list[set]:
    g = nx.from_pandas_edgelist(matched_artists_df, "artist_id_1", "artist_id_2")
    return list(nx.connected_components(g))


@app.command()
def main(
    applicative_artist_filepath: str = typer.Option(),
    applicative_product_artist_link_filepath: str = typer.Option(),
    artist_score_filepath: str = typer.Option(),
    product_embeddings_filepath: str = typer.Option(),
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_artist_alias_file_path: str = typer.Option(),
    output_delta_product_artist_link_filepath: str = typer.Option(),
) -> None:
    # 1. Load raw data
    applicative_artist_df = pd.read_parquet(applicative_artist_filepath)
    applicative_product_artist_link_df = pd.read_parquet(
        applicative_product_artist_link_filepath
    )
    artist_with_score_df = pd.read_parquet(artist_score_filepath)
    product_embeddings_df = pd.read_parquet(product_embeddings_filepath).rename(
        columns={"embeddings": "embedding"}
    )

    # 2. Preprocess data
    namesake_artist_df = get_namesakes(artist_with_score_df)
    artist_id_to_score = dict(
        artist_with_score_df.set_index(ARTIST_ID_KEY)["artist_raw_score"]
    )
    artist_id_to_name = dict(
        applicative_artist_df.set_index(ARTIST_ID_KEY)[ARTIST_NAME_KEY]
    )

    # 3. Find artists to merge
    encoder = SentenceTransformer(ENCODER_NAME, token=HF_TOKEN)
    artists_to_merge = []
    for artist_ids in tqdm.tqdm(
        namesake_artist_df.sort_values(
            by="artist_count", ascending=False
        ).artist_id_list
    ):
        offer_names_df = product_embeddings_df.loc[
            lambda df, artist_ids=artist_ids: df[ARTIST_ID_KEY].isin(artist_ids)
        ][
            [ARTIST_ID_KEY, "offer_name", "preprocessed_offer_name", "embedding"]
        ].sort_values(by=[ARTIST_ID_KEY, "preprocessed_offer_name"])
        crossed_offer_names = get_offer_name_similarities_on_df(
            encoder=encoder, offer_names_df=offer_names_df
        )
        if offer_names_df[ARTIST_ID_KEY].nunique() < 2:
            logger.warning("Not enough offer names to compare between artists.")
            continue

        if crossed_offer_names.embedding_dot.max() >= THRESHOLD:
            matched_artists_df = crossed_offer_names.loc[
                lambda df: df.embedding_dot >= THRESHOLD,
                ["artist_id_1", "artist_id_2"],
            ].drop_duplicates()

            artists_to_merge += get_artists_to_merge_for_matched_artists(
                matched_artists_df
            )
    logger.info(f"Number of artist groups to merge: {len(artists_to_merge)}")

    # %% 4. Build one to one artist mapping
    artist_mapping = {}
    for artist_ids in artists_to_merge:
        sorted_artist_ids = sorted(
            artist_ids,
            key=lambda artist_id: artist_id_to_score[artist_id],
            reverse=True,
        )
        main_artist_id = sorted_artist_ids[0]
        for duplicate_artist_id in sorted_artist_ids[1:]:
            artist_mapping[duplicate_artist_id] = main_artist_id
    artist_mapping_df = pd.DataFrame(
        {
            "duplicate_artist_id": artist_mapping.keys(),
            "main_artist_id": artist_mapping.values(),
        }
    ).assign(
        main_artist_score=lambda df: df.main_artist_id.map(artist_id_to_score),
        duplicate_artist_score=lambda df: df.duplicate_artist_id.map(
            artist_id_to_score
        ),
        main_artist_name=lambda df: df.main_artist_id.map(artist_id_to_name),
        duplicate_artist_name=lambda df: df.duplicate_artist_id.map(artist_id_to_name),
    )

    # 5. Build delta dataframes
    delta_artist_df = applicative_artist_df.loc[
        lambda df: df[ARTIST_ID_KEY].isin(artist_mapping_df.duplicate_artist_id)
    ].assign(
        **{
            ACTION_KEY: Action.remove,
            COMMENT_KEY: "merged into another artist",
        }
    )
    delta_product_artist_link_df = applicative_product_artist_link_df.loc[
        lambda df: df[ARTIST_ID_KEY].isin(artist_mapping_df.duplicate_artist_id)
    ].assign(
        **{
            ARTIST_ID_KEY: lambda df: df[ARTIST_ID_KEY].map(artist_mapping),
            ACTION_KEY: Action.update,
            COMMENT_KEY: "linked to main artist after deduplication",
        }
    )

    # 6. Save results
    delta_artist_df.to_parquet(
        output_delta_artist_file_path,
        index=False,
    )
    delta_product_artist_link_df.to_parquet(
        output_delta_product_artist_link_filepath,
        index=False,
    )
    pd.DataFrame(columns=ARTIST_ALIASES_KEYS).to_parquet(
        output_delta_artist_alias_file_path, index=False
    )


if __name__ == "__main__":
    app()
