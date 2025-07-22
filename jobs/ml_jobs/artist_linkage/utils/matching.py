import uuid

import pandas as pd

from constants import (
    ACTION_KEY,
    ARTIST_ALIASES_KEYS,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    ARTISTS_KEYS,
    COMMENT_KEY,
    DESCRIPTION_KEY,
    IMG_KEY,
    OFFER_CATEGORY_ID_KEY,
    POSTPROCESSED_ARTIST_NAME_KEY,
    PRODUCT_ID_KEY,
    PRODUCTS_KEYS,
    WIKI_ID_KEY,
    Action,
    Comment,
)
from match_artists_on_wikidata import (
    match_namesakes_per_category,
    match_per_category_no_namesakes,
)
from utils.preprocessing_utils import (
    extract_artist_name,
    prepare_artist_names_for_matching,
)


def match_artist_on_offer_names(
    products_to_link_df: pd.DataFrame,
    artist_alias_df: pd.DataFrame,
):
    """
    Match artists with products based on offer names using both raw and preprocessed matching.
    This function performs a two-step matching process:
    1. First attempts to match products with artists using raw offer names
    2. For unmatched products, applies preprocessing to artist names and attempts matching again
    Args:
        products_to_link_df (pd.DataFrame): DataFrame containing products that need to be linked
            to artists. Must contain columns for artist_name, artist_type, and offer_category_id.
        artist_alias_df (pd.DataFrame): DataFrame containing artist aliases and their corresponding
            artist_ids. Must contain columns for artist_name, artist_type, offer_category_id, and artist_id.
    Returns:
        tuple: A tuple containing three DataFrames:
            - raw_linked_products_df: Products successfully matched using raw offer names
            - preproc_linked_products_df: Products successfully matched using preprocessed offer names
            - preproc_unlinked_products_df: Products that remain unmatched after both attempts
    """

    # 1. Match artists with products to link on raw offer_names
    raw_matched_df = products_to_link_df.merge(
        artist_alias_df,
        how="left",
        on=[ARTIST_NAME_KEY, ARTIST_TYPE_KEY, OFFER_CATEGORY_ID_KEY],
    )
    raw_linked_products_df = raw_matched_df.loc[lambda df: df.artist_id.notna()]
    raw_unlinked_products_df = raw_matched_df.loc[
        lambda df: df.artist_id.isna(),
        [
            PRODUCT_ID_KEY,
            ARTIST_TYPE_KEY,
            ARTIST_NAME_KEY,
            OFFER_CATEGORY_ID_KEY,
        ],
    ]

    # 2. Match artists with products to link on preproc offer_names
    preproc_unlinked_products_df = raw_unlinked_products_df.pipe(
        prepare_artist_names_for_matching
    ).drop_duplicates()
    preproc_artist_alias_df = artist_alias_df.pipe(
        prepare_artist_names_for_matching
    ).drop_duplicates(
        subset=[
            ARTIST_ID_KEY,
            OFFER_CATEGORY_ID_KEY,
            ARTIST_TYPE_KEY,
            ARTIST_NAME_TO_MATCH_KEY,
        ]
    )
    preproc_matched_df = preproc_unlinked_products_df.merge(
        preproc_artist_alias_df.drop(columns=[ARTIST_NAME_KEY]),
        how="left",
        on=[ARTIST_NAME_TO_MATCH_KEY, ARTIST_TYPE_KEY, OFFER_CATEGORY_ID_KEY],
    )
    preproc_linked_products_df = preproc_matched_df.loc[
        lambda df: df.artist_id.notna()
    ].loc[lambda df: ~df[PRODUCT_ID_KEY].isin(raw_linked_products_df[PRODUCT_ID_KEY])]
    preproc_unlinked_products_df = (
        preproc_matched_df.loc[lambda df: df.artist_id.isna()]
        .drop_duplicates()
        .drop(columns=[ARTIST_ID_KEY])
    )

    return (
        raw_linked_products_df,
        preproc_linked_products_df,
        preproc_unlinked_products_df,
    )


def create_artists_tables(
    preproc_unlinked_products_df: pd.DataFrame,
    exploded_artist_alias_df: pd.DataFrame,
    products_to_remove_df=pd.DataFrame(),
    raw_linked_products_df=pd.DataFrame(),
    preproc_linked_products_df=pd.DataFrame(),
    artist_df=pd.DataFrame(columns=[ARTIST_ID_KEY]),
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create delta tables for products, artists, and artist aliases with action tracking.
    This function processes various product and artist dataframes to create delta tables
    that track additions and removals for database synchronization purposes.
    Args:
        preproc_unlinked_products_df (pd.DataFrame): DataFrame containing preprocessed
            products that need to be linked to new artists.
        exploded_artist_alias_df (pd.DataFrame): DataFrame containing artist information
            with exploded aliases for new artists to be created.
        products_to_remove_df (pd.DataFrame, optional): DataFrame containing products to be removed
            from the linked products table. Defaults to empty DataFrame.
        raw_linked_products_df (pd.DataFrame, optional): DataFrame containing raw products that
            have been linked to existing artists. Defaults to empty DataFrame.
        preproc_linked_products_df (pd.DataFrame, optional): DataFrame containing preprocessed
            products that have been linked to existing artists. Defaults to empty DataFrame.
        artist_df (pd.DataFrame, optional): DataFrame containing existing artists to avoid
            duplicating when creating new artists. Defaults to empty DataFrame with ARTIST_ID_KEY column.
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing:
            - delta_product_df: DataFrame with product changes including action tracking
              (add/remove) and comments describing the operation type.
            - delta_artist_df: DataFrame with new artists to be added, deduplicated
              and sorted by artist_id, wiki_id, img, and description.
            - delta_artist_alias_df: DataFrame with new artist aliases to be added,
              deduplicated.
    Note:
        All returned dataframes include 'action' and 'comment' columns for tracking
        the type of operation being performed.
    """

    delta_product_artist_link_df = pd.concat(
        [
            products_to_remove_df.filter(PRODUCTS_KEYS).assign(
                **{ACTION_KEY: Action.remove, COMMENT_KEY: Comment.removed_linked}
            ),
            raw_linked_products_df.filter(PRODUCTS_KEYS).assign(
                **{
                    ACTION_KEY: Action.add,
                    COMMENT_KEY: Comment.linked_to_existing_artist,
                }
            ),
            preproc_linked_products_df.filter(PRODUCTS_KEYS).assign(
                **{
                    ACTION_KEY: Action.add,
                    COMMENT_KEY: Comment.linked_to_existing_artist,
                }
            ),
            preproc_unlinked_products_df.merge(
                exploded_artist_alias_df,
                how="left",
                on=[
                    ARTIST_NAME_TO_MATCH_KEY,
                    ARTIST_TYPE_KEY,
                    OFFER_CATEGORY_ID_KEY,
                ],
            )
            .filter(PRODUCTS_KEYS)
            .assign(
                **{ACTION_KEY: Action.add, COMMENT_KEY: Comment.linked_to_new_artist}
            )
            .drop_duplicates(),
        ]
    )
    delta_artist_df = (
        exploded_artist_alias_df.sort_values(
            by=[
                ARTIST_ID_KEY,
                WIKI_ID_KEY,
                IMG_KEY,
                DESCRIPTION_KEY,
            ]
        )
        .assign(
            artist_name=lambda df: df[POSTPROCESSED_ARTIST_NAME_KEY]
        )  # Replace artist_name by postprocessed_artist_name so that it is well formatted
        .loc[lambda df: ~df.artist_id.isin(artist_df.artist_id.unique()), ARTISTS_KEYS]
        .drop_duplicates()
        .assign(**{ACTION_KEY: Action.add, COMMENT_KEY: Comment.new_artist})
    )
    delta_artist_alias_df = (
        exploded_artist_alias_df.loc[:, ARTIST_ALIASES_KEYS]
        .drop_duplicates()
        .assign(**{ACTION_KEY: Action.add, COMMENT_KEY: Comment.new_artist_alias})
    )
    return (
        delta_product_artist_link_df,
        delta_artist_df,
        delta_artist_alias_df,
    )


def match_artists_with_wikidata(
    new_artist_clusters_df: pd.DataFrame,
    wiki_df: pd.DataFrame,
    existing_artist_alias_df: pd.DataFrame = pd.DataFrame(
        columns=[ARTIST_ID_KEY, "artist_wiki_id"]
    ),
) -> pd.DataFrame:
    """
    Matches new artist clusters with existing Wikidata entries to assign consistent artist IDs.
    This function performs a multi-step matching process to link new artist clusters with
    Wikidata entries, handling both artists with and without namesakes. It then creates
    or reuses artist IDs to ensure consistent identification across the system.
    Args:
        new_artist_clusters_df (pd.DataFrame): DataFrame containing new artist clusters
            with artist names to be matched. Must contain ARTIST_NAME_TO_MATCH_KEY column.
        existing_artist_alias_df (pd.DataFrame | None): DataFrame containing existing artist
            aliases with their corresponding artist IDs and wiki IDs. Must contain
            ARTIST_ID_KEY and WIKI_ID_KEY columns.
        wiki_df (pd.DataFrame): DataFrame containing Wikidata information with artist
            names and metadata. Must contain 'alias_name_to_match' column.
    Returns:
        pd.DataFrame: A DataFrame with matched artists containing:
            - artist_id: Unique identifier for the artist (existing or newly generated)
            - postprocessed_artist_name: Cleaned and title-cased artist name
            - has_namesake: Boolean indicating if the artist has namesakes
            - Other relevant matching metadata
            The DataFrame is exploded by artist name and deduplicated.
    Note:
        * The function uses a "hack" approach by temporarily renaming columns to 'alias'
            to work with existing wikidata matching functions (match_namesakes_per_category
            and match_per_category_no_namesakes).
        * If none, we assume no existing artists are present.
    """
    # 1? Preprocess to use wikidata matching functions
    wiki_df = (
        wiki_df.rename(
            columns={
                "artist_name": "wiki_artist_name",
            }
        )
        .assign(
            alias_name_to_match=lambda df: df.raw_alias.apply(extract_artist_name),
            alias=lambda df: df.alias_name_to_match,
        )
        .loc[lambda df: (df.alias_name_to_match != "") & df.alias_name_to_match.notna()]
    )
    new_artist_clusters_df = new_artist_clusters_df.assign(
        alias=lambda df: df[ARTIST_NAME_TO_MATCH_KEY]
    )
    matched_namesakes_df = (
        match_namesakes_per_category(new_artist_clusters_df, wiki_df)
        .loc[lambda df: df.wiki_id.notna()]
        .assign(has_namesake=True)
    )

    # 2. Match artists on wikidata for artists with no namesake
    matched_without_namesake_df = (
        match_per_category_no_namesakes(new_artist_clusters_df, wiki_df)
        .assign(has_namesake=False)
        .loc[lambda df: ~df.tmp_id.isin(matched_namesakes_df.tmp_id.unique())]
    )

    # 3. Reconciliate matching
    matched_df = pd.concat(
        [matched_without_namesake_df, matched_namesakes_df]
    ).reset_index(drop=True)

    # 4. wiki_id to artist_id mapping
    existing_mapping_df = (
        existing_artist_alias_df.rename(columns={"artist_wiki_id": WIKI_ID_KEY})
        .loc[:, [ARTIST_ID_KEY, WIKI_ID_KEY]]
        .dropna()
        .drop_duplicates()
    )
    new_mapping_df = (
        pd.Series(
            list(set(matched_df.wiki_id).difference(set(existing_mapping_df.wiki_id)))
        )
        .to_frame(WIKI_ID_KEY)
        .assign(artist_id=lambda df: df.wiki_id.apply(lambda x: str(uuid.uuid4())))
    )
    wiki_to_artist_mapping = (
        pd.concat([existing_mapping_df, new_mapping_df])
        .dropna()
        .set_index(WIKI_ID_KEY)
        .artist_id.to_dict()
    )

    # 5. Remap matched_df with wiki_to_artist_mapping
    matched_with_ids_df = matched_df.assign(
        artist_id=lambda df: df.wiki_id.map(wiki_to_artist_mapping).fillna(df.tmp_id),
        postprocessed_artist_name=lambda df: df.wiki_artist_name.fillna(
            df[ARTIST_NAME_TO_MATCH_KEY]
        ).apply(lambda s: s.title()),
    ).sort_values(by=ARTIST_ID_KEY)

    # 6. Explode artist names and drop duplicates to output artist aliases
    return (
        matched_with_ids_df.rename(columns={"artist_name_set": ARTIST_NAME_KEY})
        .explode(ARTIST_NAME_KEY)
        .drop_duplicates()
    )
