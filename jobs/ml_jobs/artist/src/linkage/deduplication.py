import networkx as nx
import numpy as np
import pandas as pd
import tqdm
from loguru import logger
from sentence_transformers import SentenceTransformer

from src.common.constants import ARTIST_ID_KEY

THRESHOLD = 0.7
MAX_OFFERS_PER_ARTIST_FOR_COMPARISON = 1000


def get_namesakes(artist_with_stats_df: pd.DataFrame) -> pd.DataFrame:
    return (
        artist_with_stats_df.groupby("normalized_artist_name")
        .agg(
            artist_id_list=("artist_id", list),
            artist_names=("artist_name", list),
            artist_count=("artist_id", "nunique"),
            product_count=("artist_product_count", "sum"),
        )
        .loc[lambda df: df.artist_count > 1]
    )


def get_artists_to_merge(
    namesake_artist_df: pd.DataFrame,
    product_embeddings_df: pd.DataFrame,
    encoder: SentenceTransformer,
) -> list[set]:
    artists_to_merge = []
    for artist_ids in tqdm.tqdm(
        namesake_artist_df.sort_values(
            by="product_count", ascending=False
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
    return artists_to_merge


def get_offer_name_similarities_on_df(
    encoder: SentenceTransformer, offer_names_df: pd.DataFrame
) -> pd.DataFrame:
    if len(offer_names_df) > MAX_OFFERS_PER_ARTIST_FOR_COMPARISON:
        offer_names_df = (
            offer_names_df.groupby(ARTIST_ID_KEY)
            .apply(
                lambda x: x.sample(min(MAX_OFFERS_PER_ARTIST_FOR_COMPARISON, len(x))),
                include_groups=False,
            )
            .reset_index(level=0)
            .reset_index(drop=True)
        )
    try:
        similarities = encoder.similarity(
            offer_names_df.embedding.tolist(), offer_names_df.embedding.tolist()
        ).reshape(-1)
    except RuntimeError:
        similarities = np.zeros(len(offer_names_df) * len(offer_names_df)).reshape(-1)
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
