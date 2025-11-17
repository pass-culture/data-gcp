import uuid

import pandas as pd
from loguru import logger

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
    PRODUCTS_KEYS,
    WIKI_ID_KEY,
    Action,
    Comment,
)
from utils.preprocessing_utils import (
    extract_artist_name,
    prepare_artist_names_for_matching,
)

CATEGORY_MAPPING = {
    "SPECTACLE": ["music"],
    "MUSIQUE_LIVE": ["music"],
    "MUSIQUE_ENREGISTREE": ["music"],
    "LIVRE": ["book"],
    "CINEMA": ["movie"],
}


def match_artist_on_offer_names(
    products_to_link_df: pd.DataFrame,
    artist_alias_df: pd.DataFrame,
    product_artist_link_df: pd.DataFrame,
):
    """
    Match artists with products based on offer names using both raw and preprocessed matching.
    This function applies preprocessing to artist names and attempts matching.
    Args:
        products_to_link_df (pd.DataFrame): DataFrame containing products that need to be linked
            to artists. Must contain columns for artist_name, artist_type, and offer_category_id.
        artist_alias_df (pd.DataFrame): DataFrame containing artist aliases and their corresponding
            artist_ids. Must contain columns for artist_name, artist_type, offer_category_id, and artist_id.
    Returns:
        tuple: A tuple containing three DataFrames:
            - preproc_linked_products_df: Products successfully matched using preprocessed offer names
            - preproc_unlinked_products_df: Products that remain unmatched after both attempts
    """
    N_PRODUCTS_LINKED_COLUMN = "n_products_linked"

    # Preprocess artist names for matching
    logger.info(
        f"Preprocessing {len(products_to_link_df)} products and {len(artist_alias_df)} artist_aliases..."
    )
    preproc_products_to_link_df = products_to_link_df.pipe(
        prepare_artist_names_for_matching
    ).drop_duplicates()
    preproc_artist_alias_df = artist_alias_df.pipe(prepare_artist_names_for_matching)

    # Filter duplicates in artist_alias_df to keep only unique artist_name per artist_id
    artist_statistics_df = (
        product_artist_link_df.groupby("artist_id")
        .agg({"offer_product_id": "nunique"})
        .rename(columns={"offer_product_id": N_PRODUCTS_LINKED_COLUMN})
    )
    preproc_filtered_artist_alias_df = (
        preproc_artist_alias_df.merge(artist_statistics_df, on="artist_id")
        .sort_values(
            by=[
                OFFER_CATEGORY_ID_KEY,
                ARTIST_TYPE_KEY,
                ARTIST_NAME_TO_MATCH_KEY,
                N_PRODUCTS_LINKED_COLUMN,
            ],
            ascending=False,
        )
        .drop_duplicates(
            subset=[
                OFFER_CATEGORY_ID_KEY,
                ARTIST_TYPE_KEY,
                ARTIST_NAME_TO_MATCH_KEY,
            ],
            keep="first",
        )
        .drop(columns=[N_PRODUCTS_LINKED_COLUMN])
    )
    logger.info(
        f"...Done {len(preproc_products_to_link_df)} products and {len(preproc_filtered_artist_alias_df)} artist_aliases after preprocessing."
    )

    # Match artists with products to link on preproc offer_names
    logger.info("Matching products on preprocessed offer names...")
    preproc_matched_df = preproc_products_to_link_df.merge(
        preproc_filtered_artist_alias_df.drop(columns=[ARTIST_NAME_KEY]),
        how="left",
        on=[ARTIST_NAME_TO_MATCH_KEY, ARTIST_TYPE_KEY, OFFER_CATEGORY_ID_KEY],
    )
    preproc_linked_products_df = preproc_matched_df.loc[lambda df: df.artist_id.notna()]
    preproc_unlinked_products_df = (
        preproc_matched_df.loc[lambda df: df.artist_id.isna()]
        .drop_duplicates()
        .drop(columns=[ARTIST_ID_KEY])
    )

    # Logging
    logger.info(
        f"...Matched {len(preproc_linked_products_df)} products on already existing artists."
        f" {len(preproc_unlinked_products_df)} products remain unmatched."
    )

    return (preproc_linked_products_df, preproc_unlinked_products_df)


def create_artists_tables(
    preproc_unlinked_products_df: pd.DataFrame,
    exploded_artist_alias_df: pd.DataFrame,
    products_to_remove_df=pd.DataFrame(),
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

    logger.info(f"Created {len(delta_artist_df)} new artists.")
    logger.info(f"Created {len(delta_artist_alias_df)} new artist aliases.")
    logger.info(f"Created {len(delta_product_artist_link_df)} product-artist links.")

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
    logger.info(f"Matching {len(new_artist_clusters_df)} artists with Wikidata...")
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

    logger.info(
        f"...Matched {len(matched_with_ids_df.loc[lambda df: df.wiki_id.notna()].wiki_id.notna().unique())} new artist clusters with Wikidata entries."
    )

    # 6. Explode artist names and drop duplicates to output artist aliases
    return (
        matched_with_ids_df.rename(columns={"artist_name_set": ARTIST_NAME_KEY})
        .explode(ARTIST_NAME_KEY)
        .drop_duplicates()
    )


def match_per_category_no_namesakes(
    artists_df: pd.DataFrame,
    wikidata_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Matches artists from a DataFrame with Wikidata entries per category, excluding namesakes.
    Args:
        artists_df (pd.DataFrame): DataFrame containing artist data with a column 'offer_category_id' and 'alias'.
        wikidata_df (pd.DataFrame): DataFrame containing Wikidata artist data with a column 'alias' and category columns.
    Returns:
        pd.DataFrame: DataFrame with matched artists per category, excluding namesakes.
    """
    matched_df_list = []
    for pass_category, wiki_category in CATEGORY_MAPPING.items():
        # Select artist df for current category
        artists_per_category_df = artists_df.loc[
            lambda df, pass_category=pass_category: df.offer_category_id
            == pass_category
        ]

        # Select wikidata artists for current category (remove namesaked ones)
        wiki_per_category_no_ns_df = wikidata_df.loc[
            lambda df, wiki_category=wiki_category: df.loc[:, wiki_category].any(axis=1)
        ].loc[lambda df: ~df.alias.duplicated(keep=False)]

        matched_df_list.append(
            pd.merge(
                artists_per_category_df,
                wiki_per_category_no_ns_df,
                on="alias",
                how="left",
            )
        )
    return pd.concat(matched_df_list)


def match_namesakes_per_category(
    artists_df: pd.DataFrame,
    wikidata_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Matches artists with the same name (namesakes) per category between two dataframes: artists_df and wikidata_df.
    Args:
        artists_df (pd.DataFrame): DataFrame containing artist data with at least 'offer_category_id' and 'alias' columns.
        wikidata_df (pd.DataFrame): DataFrame containing Wikidata artist data with at least 'alias' column and category columns.
    Returns:
        pd.DataFrame: DataFrame containing matched artists with the best Wikidata entry per alias for each category.
    """

    def _select_best_wiki_per_alias(df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(by=["alias", "gkg"], ascending=False).drop_duplicates(
            subset="alias"
        )

    matched_df_list = []
    for pass_category, wiki_categories in CATEGORY_MAPPING.items():
        # Select artist df for current category
        artists_per_category_df = artists_df.loc[
            lambda df, pass_category=pass_category: df.offer_category_id
            == pass_category
        ]

        # Select namesaked wikidata artists for current category
        wiki_per_category_with_ns_df = (
            wikidata_df.loc[
                lambda df, wiki_categories=wiki_categories: df.loc[
                    :, wiki_categories
                ].any(axis=1)
            ]
            .loc[lambda df: df.alias.duplicated(keep=False)]
            .pipe(_select_best_wiki_per_alias)
        )

        # Select the best wiki artist per alias
        selected_wiki_per_category_df = wiki_per_category_with_ns_df.pipe(
            _select_best_wiki_per_alias
        )

        matched_df_list.append(
            pd.merge(
                artists_per_category_df,
                selected_wiki_per_category_df,
                on="alias",
                how="left",
            )
        )
    return pd.concat(matched_df_list)
