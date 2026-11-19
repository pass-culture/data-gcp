"""Business logic behind `remediate-product-links`.

Unlike `link-new-products` (incremental: only products that are new or gone are
handled), the remediation re-evaluates every existing product-artist link
against the current artist names of the product (e.g. after switching books to
Titelive contributors):
- a link whose artist is no longer named on the product is removed,
- product artist names not covered by an existing link are replayed through the
  matching (existing artists, then new artists / Wikidata).
"""

import pandas as pd
from loguru import logger

from src.common.constants import ARTIST_ID_KEY, ARTIST_NAME_KEY, WIKIDATA_ID_KEY
from src.linkage.constants import (
    ACTION_KEY,
    ARTIST_NAME_SORTED_TO_MATCH_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    PRODUCT_ID_KEY,
    PRODUCTS_KEYS,
    RAW_ALIAS_KEY,
    Action,
)
from src.linkage.preprocessing_utils import prepare_artist_names_for_matching


def anti_join_on_products_keys(
    df: pd.DataFrame, to_exclude_df: pd.DataFrame
) -> pd.DataFrame:
    """Rows of `df` whose PRODUCTS_KEYS are not in `to_exclude_df`."""
    return (
        df.merge(
            to_exclude_df.loc[:, PRODUCTS_KEYS].drop_duplicates(),
            how="left",
            on=PRODUCTS_KEYS,
            indicator=True,
        )
        .loc[lambda d: d["_merge"] == "left_only"]
        .drop(columns="_merge")
    )


def build_artist_name_keys_df(
    artist_df: pd.DataFrame,
    wiki_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build every (artist_id, order-insensitive name key) an artist is known by.

    Keys come from the artist canonical name and, for artists having a
    wikidata_id, from all their Wikidata aliases. This prevents dropping a
    valid link when the canonical name was replaced by the Wikidata label
    (e.g. "Fiodor Dostoïevski") while the product lists another spelling
    (e.g. "Fedor Dostoïevski").
    """
    name_frames = [artist_df.loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY]]]
    if (
        wiki_df is not None
        and WIKIDATA_ID_KEY in artist_df.columns
        and {WIKIDATA_ID_KEY, RAW_ALIAS_KEY}.issubset(wiki_df.columns)
    ):
        name_frames.append(
            artist_df.loc[
                lambda df: df[WIKIDATA_ID_KEY].notna(), [ARTIST_ID_KEY, WIKIDATA_ID_KEY]
            ]
            .merge(
                wiki_df.loc[:, [WIKIDATA_ID_KEY, RAW_ALIAS_KEY]].drop_duplicates(),
                how="inner",
                on=WIKIDATA_ID_KEY,
            )
            .rename(columns={RAW_ALIAS_KEY: ARTIST_NAME_KEY})
            .loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY]]
        )

    return (
        pd.concat(name_frames, ignore_index=True)
        .drop_duplicates()
        .pipe(prepare_artist_names_for_matching)
        .loc[:, [ARTIST_ID_KEY, ARTIST_NAME_SORTED_TO_MATCH_KEY]]
        .drop_duplicates()
    )


def get_links_to_remediate_df(
    products_df: pd.DataFrame,
    product_artist_link_df: pd.DataFrame,
    artist_df: pd.DataFrame,
    wiki_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Re-evaluate existing product-artist links against current product artist names.

    A link is kept if any name the linked artist is known by (canonical name or
    Wikidata alias, word-order-insensitive) matches an artist name of the product
    for the same artist_type.

    Args:
        products_df: current products with artist names.
        product_artist_link_df: existing product-artist links.
        artist_df: existing artists with artist name and optionally wikidata_id.
        wiki_df: Wikidata dump with wikidata_id and raw_alias columns.

    Returns:
        - products_to_remove_df: existing links not confirmed by a current artist name.
        - products_to_link_df: product artist names not covered by an existing link,
          to be replayed through the matching.
    """
    merge_columns = [PRODUCT_ID_KEY, ARTIST_TYPE_KEY, ARTIST_NAME_SORTED_TO_MATCH_KEY]

    preproc_products_df = products_df.pipe(prepare_artist_names_for_matching)
    artist_name_keys_df = build_artist_name_keys_df(artist_df, wiki_df)

    actual_product_ids = (
        preproc_products_df.loc[:, merge_columns]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    linked_products = (
        product_artist_link_df.loc[:, PRODUCTS_KEYS]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    matched_df = actual_product_ids.merge(
        linked_products.merge(artist_name_keys_df, how="inner", on=ARTIST_ID_KEY),
        how="inner",
        on=merge_columns,
    )
    matched_links_df = matched_df.loc[:, PRODUCTS_KEYS].drop_duplicates()
    matched_product_ids = matched_df.loc[:, merge_columns].drop_duplicates()

    products_to_remove_df = (
        anti_join_on_products_keys(linked_products, matched_links_df)
        .loc[:, PRODUCTS_KEYS]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    products_to_link_df = (
        actual_product_ids.merge(
            matched_product_ids, how="left", on=merge_columns, indicator=True
        )
        .loc[lambda df: df["_merge"] == "left_only", merge_columns]
        .merge(preproc_products_df, how="left", on=merge_columns)
        .drop(columns=[ARTIST_NAME_TO_MATCH_KEY, ARTIST_NAME_SORTED_TO_MATCH_KEY])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    logger.info(
        f"[Remediation] Links to remove: {len(products_to_remove_df)}, "
        f"Products to link: {len(products_to_link_df)}, "
        f"Links kept: {len(matched_links_df)}"
    )

    return products_to_remove_df, products_to_link_df


def exclude_unchanged_links(
    delta_product_df: pd.DataFrame,
    product_artist_link_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Drop no-op operations from the product-artist link delta.

    Since existing links are replayed through the matching:
    - a removed link that is re-added (same product, artist and type) is still
      valid: neither remove nor re-add it,
    - an added link that already exists must not be re-added.
    """
    removed_df = delta_product_df.loc[lambda df: df[ACTION_KEY] == Action.remove]
    added_df = delta_product_df.loc[lambda df: df[ACTION_KEY] == Action.add]
    other_df = delta_product_df.loc[
        lambda df: ~df[ACTION_KEY].isin([Action.remove, Action.add])
    ]

    return pd.concat(
        [
            anti_join_on_products_keys(removed_df, added_df),
            anti_join_on_products_keys(added_df, product_artist_link_df),
            other_df,
        ],
        ignore_index=True,
    )
