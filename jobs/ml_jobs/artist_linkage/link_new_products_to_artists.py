import uuid

import pandas as pd
import typer
from loguru import logger

from match_artists_on_wikidata import (
    load_wikidata,
    match_namesakes_per_category,
    match_per_category_no_namesakes,
)
from utils.preprocessing_utils import ARTIST_NAME_TO_FILTER, extract_artist_name

# Columns
ARTIST_ID_KEY = "artist_id"
ID_KEY = "id"
PRODUCT_ID_KEY = "offer_product_id"
ARTIST_NAME_KEY = "artist_name"
ARTIST_NAME_TO_MATCH_KEY = "artist_name_to_match"
ARTIST_TYPE_KEY = "artist_type"
WIKI_ID_KEY = "wiki_id"
OFFER_CATEGORY_ID_KEY = "offer_category_id"
ID_PER_CATEGORY = "id_per_category"

NOT_MATCHED_WITH_ARTISTS_KEY = "not_matched_with_artists"
REMOVED_PRODUCTS_KEY = "removed_products"
MATCHED_WITH_ARTISTS_KEY = "matched_with_artists"

ARTIST_ALIASES_KEYS = [
    ARTIST_ID_KEY,
    OFFER_CATEGORY_ID_KEY,
    ARTIST_TYPE_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    WIKI_ID_KEY,
]
ARTISTS_KEYS = [ARTIST_ID_KEY, ARTIST_NAME_KEY, "description", "img"]
PRODUCTS_KEYS = [
    PRODUCT_ID_KEY,
    ARTIST_ID_KEY,
    ARTIST_TYPE_KEY,
]


def get_products_to_remove_and_link_df(
    products_df: pd.DataFrame,
    product_artist_link_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Identify products that need to be removed from artist links and new products that need to be linked.
    This function performs an outer merge between current products and existing product-artist links
    to determine which products are no longer valid (should be removed) and which new products
    need to be linked to artists.
    Args:
        products_df (pd.DataFrame): DataFrame containing current products with merge columns
        product_artist_link_df (pd.DataFrame): DataFrame containing existing product-artist links
            with merge columns and artist ID
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing:
            - products_to_remove_df: DataFrame with products that exist in links but not in
              current products (should be removed from artist links)
            - products_to_link_df: DataFrame with new products that need to be linked to artists
    Side Effects:
        Logs information about the number of products to remove and link, along with merge statistics
    """
    MERGE_COLUMNS = [PRODUCT_ID_KEY, ARTIST_TYPE_KEY]

    actual_product_ids = products_df.loc[:, MERGE_COLUMNS].reset_index(drop=True)
    linked_product_ids = (
        product_artist_link_df.loc[:, MERGE_COLUMNS + [ARTIST_ID_KEY]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    merged_df = actual_product_ids.merge(
        linked_product_ids,
        how="outer",
        left_on=MERGE_COLUMNS,
        right_on=MERGE_COLUMNS,
        indicator=True,
    ).replace(
        {
            "_merge": {
                "left_only": NOT_MATCHED_WITH_ARTISTS_KEY,
                "right_only": REMOVED_PRODUCTS_KEY,
                "both": MATCHED_WITH_ARTISTS_KEY,
            }
        }
    )

    products_to_remove_df = merged_df.loc[
        lambda df: df._merge == REMOVED_PRODUCTS_KEY,
        MERGE_COLUMNS + [ARTIST_ID_KEY],
    ]
    products_to_link_df = merged_df.loc[
        lambda df: df._merge == NOT_MATCHED_WITH_ARTISTS_KEY,
        MERGE_COLUMNS,
    ].merge(products_df, how="left", on=MERGE_COLUMNS)

    merge_stats_df = merged_df._merge.value_counts().reset_index()

    logger.info(
        f"Products to remove: {len(products_to_remove_df)}, "
        f"Products to link: {len(products_to_link_df)}",
        extra={
            "merge_stats": merge_stats_df.to_dict(orient="records"),
        },
    )

    return products_to_remove_df, products_to_link_df


def build_artist_alias(
    product_df: pd.DataFrame,
    product_artist_link_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build an artist alias DataFrame by merging product and artist link data.
    This function creates a deduplicated artist alias table by joining product
    information with artist link data, extracting relevant artist details along
    with their associated offer categories.
    Args:
        product_df (pd.DataFrame): DataFrame containing product information with
            columns including PRODUCT_ID_KEY, ARTIST_TYPE_KEY, ARTIST_NAME_KEY,
            and OFFER_CATEGORY_ID_KEY.
        product_artist_link_df (pd.DataFrame): DataFrame containing artist-product
            linkages with columns including PRODUCT_ID_KEY, ARTIST_TYPE_KEY, and
            ARTIST_ID_KEY.
    Returns:
        pd.DataFrame: A deduplicated and sorted DataFrame containing artist alias
            information with columns: ARTIST_ID_KEY, ARTIST_NAME_KEY,
            ARTIST_TYPE_KEY, and OFFER_CATEGORY_ID_KEY.
    Notes:
        We don't use data from artist_alias_table because it is inconsistent (missing artist_type) and will be soon removed.
    """

    product_with_names_df = product_artist_link_df.merge(
        product_df,
        how="left",
        left_on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
        right_on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
    )

    return (
        product_with_names_df.loc[
            :, [ARTIST_ID_KEY, ARTIST_NAME_KEY, ARTIST_TYPE_KEY, OFFER_CATEGORY_ID_KEY]
        ]
        .drop_duplicates()
        .sort_values(
            by=[ARTIST_ID_KEY, ARTIST_NAME_KEY, ARTIST_TYPE_KEY, OFFER_CATEGORY_ID_KEY]
        )
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
    preproc_unlinked_products_df = (
        raw_unlinked_products_df.assign(
            artist_name_to_match=lambda df: df.artist_name.apply(extract_artist_name),
        )
        .loc[
            lambda df: (df.artist_name_to_match != "") & df.artist_name_to_match.notna()
        ]
        .drop_duplicates()
    )
    preproc_artist_alias_df = (
        artist_alias_df.assign(
            artist_name_to_match=lambda df: df.artist_name.apply(extract_artist_name),
        )
        .loc[
            lambda df: (df.artist_name_to_match != "") & df.artist_name_to_match.notna()
        ]
        .drop_duplicates(
            subset=[
                ARTIST_ID_KEY,
                OFFER_CATEGORY_ID_KEY,
                ARTIST_TYPE_KEY,
                ARTIST_NAME_TO_MATCH_KEY,
            ]
        )
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


def match_new_artists_with_wikidata(
    new_artist_clusters_df: pd.DataFrame,
    existing_artist_alias_df: pd.DataFrame,
    wiki_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Matches new artist clusters with existing Wikidata entries to assign consistent artist IDs.
    This function performs a multi-step matching process to link new artist clusters with
    Wikidata entries, handling both artists with and without namesakes. It then creates
    or reuses artist IDs to ensure consistent identification across the system.
    Args:
        new_artist_clusters_df (pd.DataFrame): DataFrame containing new artist clusters
            with artist names to be matched. Must contain ARTIST_NAME_TO_MATCH_KEY column.
        existing_artist_alias_df (pd.DataFrame): DataFrame containing existing artist
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
        The function uses a "hack" approach by temporarily renaming columns to 'alias'
        to work with existing wikidata matching functions (match_namesakes_per_category
        and match_per_category_no_namesakes).
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


def create_delta_tables(
    products_to_remove_df: pd.DataFrame,
    raw_linked_products_df: pd.DataFrame,
    preproc_linked_products_df: pd.DataFrame,
    preproc_unlinked_products_df: pd.DataFrame,
    exploded_artist_alias_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create delta tables for products, artists, and artist aliases with action tracking.
    This function processes various product and artist dataframes to create delta tables
    that track additions and removals for database synchronization purposes.
    Args:
        products_to_remove_df (pd.DataFrame): DataFrame containing products to be removed
            from the linked products table.
        raw_linked_products_df (pd.DataFrame): DataFrame containing raw products that
            have been linked to existing artists.
        preproc_linked_products_df (pd.DataFrame): DataFrame containing preprocessed
            products that have been linked to existing artists.
        preproc_unlinked_products_df (pd.DataFrame): DataFrame containing preprocessed
            products that need to be linked to new artists.
        exploded_artist_alias_df (pd.DataFrame): DataFrame containing artist information
            with exploded aliases for new artists to be created.
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

    delta_product_df = pd.concat(
        [
            products_to_remove_df.filter(PRODUCTS_KEYS).assign(
                action="remove", comment="removed linked"
            ),
            raw_linked_products_df.filter(PRODUCTS_KEYS).assign(
                action="add", comment="linked to existing artist"
            ),
            preproc_linked_products_df.filter(PRODUCTS_KEYS).assign(
                action="add", comment="linked to existing artist"
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
            .assign(action="add", comment="linked to new artist")
            .drop_duplicates(),
        ]
    )
    delta_artist_df = (
        exploded_artist_alias_df.sort_values(
            by=[
                ARTIST_ID_KEY,
                WIKI_ID_KEY,
                "img",
                "description",
            ]
        )
        .loc[:, ARTISTS_KEYS]
        .drop_duplicates()
        .assign(action="add", comment="new artist")
    )
    delta_artist_alias_df = (
        exploded_artist_alias_df.loc[:, ARTIST_ALIASES_KEYS]
        .drop_duplicates()
        .assign(action="add", comment="new artist alias")
    )
    return (
        delta_product_df,
        delta_artist_df,
        delta_artist_alias_df,
    )


def sanity_checks(
    delta_product_df: pd.DataFrame,
    delta_artist_df: pd.DataFrame,
    delta_artist_alias_df: pd.DataFrame,
    artist_df: pd.DataFrame,
    artist_alias_df: pd.DataFrame,
) -> None:
    """
    Perform sanity checks on delta dataframes before updating the database.
    This function validates that:
    1. No duplicate entries exist in any delta dataframe
    2. All products have been successfully linked to artists (no null artist_id)
    3. No new artists being added already exist in the database
    4. No new artist aliases being added already exist in the database
    Args:
        delta_product_df (pd.DataFrame): DataFrame containing new/updated products
        delta_artist_df (pd.DataFrame): DataFrame containing new artists to be added
        delta_artist_alias_df (pd.DataFrame): DataFrame containing new artist aliases
        artist_df (pd.DataFrame): Existing artists in the database
        artist_alias_df (pd.DataFrame): Existing artist aliases in the database
    Returns:
        None
    Raises:
        AssertionError: If duplicate entries are found in any delta dataframe
        ValueError: If any of the following conditions are met:
            - Products with no artist_id after matching
            - Artists that already exist in the database
            - Artist aliases that already exist in the database
    """

    # 1. Product Artist Links
    unmatched_products = delta_product_df.loc[lambda df: df.artist_id.isna()]
    if len(unmatched_products) > 0:
        print("There are products with no artist_id after matching.")
        print(unmatched_products)
        raise ValueError(
            "There are still products that could not be linked to artists after matching."
        )
    assert (
        not delta_product_df.drop(columns=["action", "comment"]).duplicated().any()
    ), "Duplicate entries in delta_product_df"

    # 2. Artists
    recreated_artist_ids = delta_artist_df.loc[
        lambda df: df.artist_id.isin(artist_df.artist_id)
    ]
    if len(recreated_artist_ids) > 0:
        print("Found existing artists after matching.")
        print(recreated_artist_ids)
        raise ValueError(
            "There are artists that already exist in the database after matching."
        )
    assert (
        not delta_artist_df.drop(columns=["action", "comment"]).duplicated().any()
    ), "Duplicate entries in delta_artist_df"

    # 3. Artist Aliases
    already_existing_artist_aliases = delta_artist_alias_df.merge(
        artist_alias_df,
        how="outer",
        on=[ARTIST_ID_KEY, ARTIST_NAME_KEY, ARTIST_TYPE_KEY],
        indicator=True,
    ).loc[lambda df: df._merge == "both"]
    if len(already_existing_artist_aliases) > 0:
        print("Found existing artist aliases after matching.")
        print(already_existing_artist_aliases)
        raise ValueError(
            "There are artist aliases that already exist in the database after matching."
        )
    assert (
        not delta_artist_alias_df.drop(columns=["action", "comment"]).duplicated().any()
    ), "Duplicate entries in delta_artist_alias_df"

    # Additional checks can be added as needed


app = typer.Typer()


@app.command()
def main(
    # Input files
    artist_filepath: str = typer.Option(),
    artist_alias_file_path: str = typer.Option(),
    product_artist_link_filepath: str = typer.Option(),
    product_filepath: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    # Output files
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_artist_alias_file_path: str = typer.Option(),
    output_delta_product_artist_link_filepath: str = typer.Option(),
) -> None:
    # 1. Load data
    product_artist_link_df = pd.read_parquet(product_artist_link_filepath).astype(
        {PRODUCT_ID_KEY: int}
    )
    product_df = (
        pd.read_parquet(product_filepath)
        .astype({PRODUCT_ID_KEY: int})
        .loc[lambda df: ~df.artist_name.isin(ARTIST_NAME_TO_FILTER)]
    )
    artist_df = pd.read_parquet(artist_filepath)
    existing_artist_alias_df = pd.read_parquet(artist_alias_file_path)
    wiki_df = load_wikidata(
        wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name
    ).reset_index(drop=True)
    artist_alias_df = build_artist_alias(product_df, product_artist_link_df)

    # 2. Split products between to remove and to link
    products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
        product_df, product_artist_link_df
    )

    # 3. Match products to link with artists on both raw and preprocessed offer names
    raw_linked_products_df, preproc_linked_products_df, preproc_unlinked_products_df = (
        match_artist_on_offer_names(products_to_link_df, artist_alias_df)
    )

    # 4. Create new artist clusters by offer_category and artist type
    new_artist_clusters_df = (
        preproc_unlinked_products_df.groupby(
            [OFFER_CATEGORY_ID_KEY, ARTIST_TYPE_KEY, ARTIST_NAME_TO_MATCH_KEY]
        )
        .agg(
            tmp_id=(ARTIST_NAME_KEY, lambda x: str(uuid.uuid4())),
            artist_name_set=(ARTIST_NAME_KEY, lambda x: set(x.unique())),
            artist_name_count=(ARTIST_NAME_KEY, "count"),
            artist_name_nunique=(ARTIST_NAME_KEY, "nunique"),
        )
        .reset_index()
    )

    # 5. Match new artist clusters with existing artists on Wikidata
    exploded_artist_alias_df = match_new_artists_with_wikidata(
        new_artist_clusters_df,
        existing_artist_alias_df,
        wiki_df,
    )

    # 6. Create new artists and artist aliases
    delta_product_df, delta_artist_df, delta_artist_alias_df = create_delta_tables(
        products_to_remove_df,
        raw_linked_products_df,
        preproc_linked_products_df,
        preproc_unlinked_products_df,
        exploded_artist_alias_df,
    )

    # 7. Sanity check for consistency
    sanity_checks(
        delta_product_df,
        delta_artist_df,
        delta_artist_alias_df,
        artist_df,
        artist_alias_df,
    )

    # 7. Save files
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    delta_artist_alias_df.to_parquet(output_delta_artist_alias_file_path, index=False)
    delta_product_df.to_parquet(output_delta_product_artist_link_filepath, index=False)


if __name__ == "__main__":
    app()
