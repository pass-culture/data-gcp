# %%
import uuid

import pandas as pd
import typer
from loguru import logger

from constants import (
    ACTION_KEY,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    COMMENT_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
    PRODUCTS_KEYS,
    ProductToLinkStatus,
)
from utils.loading import load_wikidata
from utils.matching import (
    create_artists_tables,
    match_artist_on_offer_names,
    match_artists_with_wikidata,
)
from utils.preprocessing_utils import filter_products

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
    products_to_link_df = merged_df.loc[
        lambda df: df._merge == ProductToLinkStatus.not_matched_with_artists_key,
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
        how="inner",
        left_on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
        right_on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
    )

    return (
        product_with_names_df.loc[:, ALIAS_MERGE_COLUMNS]
        .drop_duplicates()
        .sort_values(by=ALIAS_MERGE_COLUMNS)
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
        logger.error("There are products with no artist_id after matching.")
        logger.error(unmatched_products)
        raise ValueError(
            "There are still products that could not be linked to artists after matching."
        )
    assert (
        not delta_product_df.drop(columns=[ACTION_KEY, COMMENT_KEY]).duplicated().any()
    ), "Duplicate entries in delta_product_df"
    delta_to_check_for_duplicates = delta_product_df.loc[
        lambda df: df.action != "remove"
    ]  # TODO: Remove this filter when duplicates product links have been cleaned up in production
    assert len(
        delta_to_check_for_duplicates.drop_duplicates(
            ["offer_product_id", "artist_type"]
        )
    ) == len(
        delta_to_check_for_duplicates
    ), "Duplicate offer_product_id and artist_type combinations in delta_product_df"

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

    # 3. Artist Aliases
    already_existing_artist_aliases = delta_artist_alias_df.merge(
        artist_alias_df,
        how="outer",
        on=ALIAS_MERGE_COLUMNS,
        indicator=True,
    ).loc[lambda df: df._merge == "both"]
    if len(already_existing_artist_aliases) > 0:
        logger.error("Found existing artist aliases after matching.")
        logger.error(already_existing_artist_aliases)
        raise ValueError(
            "There are artist aliases that already exist in the database after matching."
        )
    assert (
        not delta_artist_alias_df.drop(columns=[ACTION_KEY, COMMENT_KEY])
        .duplicated()
        .any()
    ), "Duplicate entries in delta_artist_alias_df"


# %%
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
        .pipe(filter_products)
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
    preproc_linked_products_df, preproc_unlinked_products_df = (
        match_artist_on_offer_names(
            products_to_link_df=products_to_link_df,
            artist_alias_df=artist_alias_df,
            product_artist_link_df=product_artist_link_df,
        )
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
    logger.info(
        f"Created {len(new_artist_clusters_df)} new artist clusters from {len(preproc_unlinked_products_df)} unlinked products."
    )

    # 5. Match new artist clusters with existing artists on Wikidata
    exploded_artist_alias_df = match_artists_with_wikidata(
        new_artist_clusters_df=new_artist_clusters_df,
        wiki_df=wiki_df,
        existing_artist_alias_df=existing_artist_alias_df,
    )

    # 6. Create new artists and artist aliases
    delta_product_df, delta_artist_df, delta_artist_alias_df = create_artists_tables(
        preproc_unlinked_products_df=preproc_unlinked_products_df,
        exploded_artist_alias_df=exploded_artist_alias_df,
        products_to_remove_df=products_to_remove_df,
        preproc_linked_products_df=preproc_linked_products_df,
        artist_df=artist_df,
    )

    # 7. Sanity check for consistency
    sanity_checks(
        delta_product_df,
        delta_artist_df,
        delta_artist_alias_df,
        artist_df,
        artist_alias_df,
    )

    # 8. Save files
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    delta_artist_alias_df.to_parquet(output_delta_artist_alias_file_path, index=False)
    delta_product_df.to_parquet(output_delta_product_artist_link_filepath, index=False)


if __name__ == "__main__":
    app()
