import pandas as pd
from loguru import logger

from src.common.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
)
from src.linkage.constants import (
    ACTION_KEY,
    ARTIST_TYPE_KEY,
    COMMENT_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
    PRODUCTS_KEYS,
    ProductToLinkStatus,
)

ALIAS_MERGE_COLUMNS = [
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
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
        product_artist_link_df.loc[:, PRODUCTS_KEYS]
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
                "left_only": ProductToLinkStatus.not_matched_with_artists_key,
                "right_only": ProductToLinkStatus.removed_products_key,
                "both": ProductToLinkStatus.matched_with_artists_key,
            }
        }
    )

    products_to_remove_df = merged_df.loc[
        lambda df: df._merge == ProductToLinkStatus.removed_products_key,
        PRODUCTS_KEYS,
    ]
    products_to_link_df = (
        merged_df.loc[
            lambda df: df._merge == ProductToLinkStatus.not_matched_with_artists_key,
            MERGE_COLUMNS,
        ]
        .merge(products_df, how="left", on=MERGE_COLUMNS)
        .drop_duplicates()
    )

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
    artist_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine artist names from artist_df and product_df to create a comprehensive alias dataframe."""
    artist_alias_from_artist_names_df = (
        product_artist_link_df.merge(
            artist_df.assign(
                artist_name=lambda df: df[ARTIST_NAME_KEY].str.lower()
            ).loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY]],
            how="left",
            on=ARTIST_ID_KEY,
            validate="many_to_one",
        )
        .merge(
            product_df.loc[
                :, [PRODUCT_ID_KEY, OFFER_CATEGORY_ID_KEY]
            ].drop_duplicates(),
            how="left",
            on=[PRODUCT_ID_KEY],
            validate="many_to_one",
        )
        .loc[:, ALIAS_MERGE_COLUMNS]
        .drop_duplicates()
    )

    # Use artist names from products when we have a clear 1:1 mapping between product and artist
    safe_product_df = product_df.loc[
        lambda df: ~df.duplicated(subset=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY], keep=False)
    ]
    product_with_names_df = product_artist_link_df.merge(
        safe_product_df,
        how="inner",
        left_on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
        right_on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
        validate="many_to_one",
    )
    artist_alias_based_on_products_df = (
        product_with_names_df.loc[:, ALIAS_MERGE_COLUMNS]
        .drop_duplicates()
        .sort_values(by=ALIAS_MERGE_COLUMNS)
    )

    # Combine both sources of artist aliases and remove duplicates
    return (
        pd.concat(
            [
                artist_alias_from_artist_names_df,
                artist_alias_based_on_products_df,
            ],
            axis=0,
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )


def sanity_check_product_links(
    delta_product_df: pd.DataFrame,
    delta_artist_df: pd.DataFrame,
    artist_df: pd.DataFrame,
) -> None:
    """
    Perform sanity checks on delta dataframes before updating the database.
    This function validates that:
    1. No duplicate entries exist in any delta dataframe
    2. All products have been successfully linked to artists (no null artist_id)
    3. No new artists being added already exist in the database
    Args:
        delta_product_df (pd.DataFrame): DataFrame containing new/updated products
        delta_artist_df (pd.DataFrame): DataFrame containing new artists to be added
        artist_df (pd.DataFrame): Existing artists in the database
    Returns:
        None
    Raises:
        AssertionError: If duplicate entries are found in any delta dataframe
        ValueError: If any of the following conditions are met:
            - Products with no artist_id after matching
            - Artists that already exist in the database
    """

    # 1. Product Artist Links
    unmatched_products = delta_product_df.loc[lambda df: df.artist_id.isna()]
    if len(unmatched_products) > 0:
        logger.error("There are products with no artist_id after matching.")
        logger.error(unmatched_products)
        raise ValueError(
            "There are still products that could not be linked to artists after matching."
        )
    assert (
        not delta_product_df.drop(columns=[ACTION_KEY, COMMENT_KEY]).duplicated().any()
    ), "Duplicate entries in delta_product_df"

    # 2. Artists
    recreated_artist_ids = delta_artist_df.loc[
        lambda df: df.artist_id.isin(artist_df.artist_id)
    ]
    if len(recreated_artist_ids) > 0:
        logger.error("Found existing artists after matching.")
        logger.error(recreated_artist_ids)
        raise ValueError(
            "There are artists that already exist in the database after matching."
        )
    assert (
        not delta_artist_df.drop(columns=[ACTION_KEY, COMMENT_KEY]).duplicated().any()
    ), "Duplicate entries in delta_artist_df"
