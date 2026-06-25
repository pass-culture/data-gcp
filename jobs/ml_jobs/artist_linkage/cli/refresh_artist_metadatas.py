import os

import pandas as pd
import typer
from loguru import logger

from src.constants import (
    ACTION_KEY,
    ARTIST_ALIASES_KEYS,
    ARTIST_DESCRIPTION_KEY,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_PRO_SEARCH_SCORE_KEY,
    ARTIST_TYPE_KEY,
    ARTIST_WIKI_ID_KEY,
    ARTISTS_KEYS,
    COMMENT_KEY,
    IMG_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
    PRODUCTS_KEYS,
    WIKIDATA_ID_KEY,
    Action,
)
from src.utils.loading import load_wikidata
from src.utils.matching import perform_wikidata_category_matching
from src.utils.preprocessing_utils import (
    filter_products,
    prepare_artist_names_for_matching,
)


def retrieve_artist_wikidata_id(
    artist_df: pd.DataFrame, artist_alias_df: pd.DataFrame
) -> pd.DataFrame:
    """Retrieve and merge wikidata IDs for artists from the artist alias table.

    This function checks for duplicate wikidata IDs per artist and raises an error
    if found, then merges the first wikidata ID for each artist back to the main
    artist dataframe.

    Args:
        artist_df (pd.DataFrame): Main artist dataframe containing artist information.
        artist_alias_df (pd.DataFrame): Artist alias dataframe containing wikidata IDs
            in the 'artist_wiki_id' column.

    Returns:
        pd.DataFrame: Artist dataframe with wikidata IDs merged in, containing only
            artists that have wikidata IDs.

    Raises:
        ValueError: If any artist has multiple different wikidata IDs.
    """
    # Check for duplicate wikidata IDs before processing
    wiki_ids_per_artists = (
        artist_alias_df[artist_alias_df[ARTIST_WIKI_ID_KEY].notna()]
        .groupby(ARTIST_ID_KEY)[ARTIST_WIKI_ID_KEY]
        .nunique()
    )
    if (wiki_ids_per_artists > 1).any():
        duplicated_artists = wiki_ids_per_artists[
            wiki_ids_per_artists > 1
        ].index.tolist()
        raise ValueError(
            f"Artists with multiple wikidata IDs found. 5 first artists: {duplicated_artists.head(min(5, len(duplicated_artists)))}"
        )

    artist_id_with_wikidata_ids = (
        artist_alias_df[artist_alias_df[ARTIST_WIKI_ID_KEY].notna()]
        .groupby(ARTIST_ID_KEY)[ARTIST_WIKI_ID_KEY]
        .first()
        .to_frame(name=WIKIDATA_ID_KEY)
    )

    return artist_df.merge(
        artist_id_with_wikidata_ids,
        how="inner",
        on=ARTIST_ID_KEY,
    )


def create_delta_df_for_metadata_refresh(
    refreshed_artists_df: pd.DataFrame,
    newly_matched_artists_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create delta dataframes for metadata refresh operation.

    This function creates the necessary delta dataframes for updating artist metadata,
    including an empty product link dataframe and empty artist alias dataframe since
    only artist metadata is being refreshed.

    Args:
        refreshed_artists_df (pd.DataFrame): Dataframe containing artists with
            refreshed metadata from wikidata.
        newly_matched_artists_df (pd.DataFrame): Dataframe containing newly matched
            artists with metadata from wikidata.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing:
            - empty_delta_product_artist_link_df: Empty dataframe for product links
            - delta_artist_df: Dataframe with artists to update and metadata
            - empty_delta_artist_alias_df: Empty dataframe for artist aliases
    """
    delta_already_matched = refreshed_artists_df.loc[:, ARTISTS_KEYS].assign(
        **{
            ACTION_KEY: Action.update,
            COMMENT_KEY: "artist with refreshed metadata from wikidata",
        }
    )
    delta_newly_matched = newly_matched_artists_df.assign(
        **{
            ACTION_KEY: Action.update,
            COMMENT_KEY: "artist matched and refreshed from wikidata",
        }
    )
    delta_artist_df = pd.concat(
        [delta_already_matched, delta_newly_matched], ignore_index=True
    )
    empty_delta_product_artist_link_df = pd.DataFrame(
        columns=[*PRODUCTS_KEYS, ACTION_KEY, COMMENT_KEY]
    )
    empty_delta_artist_alias_df = pd.DataFrame(
        columns=[*ARTIST_ALIASES_KEYS, ACTION_KEY, COMMENT_KEY]
    )
    return (
        empty_delta_product_artist_link_df,
        delta_artist_df,
        empty_delta_artist_alias_df,
    )


def match_unmatched_artists_with_wikidata(
    applicative_artist_df: pd.DataFrame,
    artist_with_wikidata_ids_df: pd.DataFrame,
    product_artist_link_filepath: str,
    product_filepath: str,
    wiki_df: pd.DataFrame,
) -> pd.DataFrame:
    """Match unmatched existing artists onto Wikidata.

    Args:
        applicative_artist_df (pd.DataFrame): Current applicative database artists.
            Must contain ARTIST_PRO_SEARCH_SCORE_KEY column.
        artist_with_wikidata_ids_df (pd.DataFrame): Artists with wikidata_id already assigned.
        product_artist_link_filepath (str): Path to product artist links.
        product_filepath (str): Path to products.
        wiki_df (pd.DataFrame): Wikidata dump.

    Returns:
        pd.DataFrame: Newly matched artists to update, projected to ARTISTS_KEYS.
    """
    artists_without_wiki_ids_df = applicative_artist_df.loc[
        lambda df: df[WIKIDATA_ID_KEY].isna()
    ]
    if len(artists_without_wiki_ids_df) == 0:
        return pd.DataFrame(columns=ARTISTS_KEYS)

    # Derive (artist_id → offer_category_id, artist_type) from product/link data
    product_artist_link_df = pd.read_parquet(product_artist_link_filepath).astype(
        {PRODUCT_ID_KEY: int}
    )
    product_df = (
        pd.read_parquet(product_filepath)
        .astype({PRODUCT_ID_KEY: int})
        .pipe(filter_products)
    )
    artist_category_type_df = (
        product_artist_link_df.merge(
            product_df[[PRODUCT_ID_KEY, OFFER_CATEGORY_ID_KEY]].drop_duplicates(),
            on=PRODUCT_ID_KEY,
            how="inner",
        )
        .loc[:, [ARTIST_ID_KEY, ARTIST_TYPE_KEY, OFFER_CATEGORY_ID_KEY]]
        .drop_duplicates()
    )

    # Enrich unmatched artists with offer_category and artist_type
    artists_to_match_df = artists_without_wiki_ids_df[
        [ARTIST_ID_KEY, ARTIST_NAME_KEY, ARTIST_PRO_SEARCH_SCORE_KEY]
    ].merge(
        artist_category_type_df,
        on=ARTIST_ID_KEY,
        how="inner",
    )

    # Preprocess the actual artist name for wikidata matching
    preproc_artists_df = prepare_artist_names_for_matching(artists_to_match_df)
    if len(preproc_artists_df) == 0:
        return pd.DataFrame(columns=ARTISTS_KEYS)

    unmatched_artist_clusters_df = preproc_artists_df.drop_duplicates(
        subset=[
            ARTIST_ID_KEY,
            OFFER_CATEGORY_ID_KEY,
            ARTIST_TYPE_KEY,
            ARTIST_NAME_TO_MATCH_KEY,
        ]
    ).assign(
        artist_name_set=lambda df: df[ARTIST_NAME_KEY].apply(
            lambda x: {x}
        ),  # for perform_wikidata_category_matching compatibility
        artist_name_count=1,
        artist_name_nunique=1,
    )

    matched_df = perform_wikidata_category_matching(
        unmatched_artist_clusters_df, wiki_df
    ).loc[lambda df: df[WIKIDATA_ID_KEY].notna()]
    if len(matched_df) == 0:
        return pd.DataFrame(columns=ARTISTS_KEYS)

    # Constraint 1: exclude wikidata_ids already used in the database
    already_used_wikidata_ids = set(
        artist_with_wikidata_ids_df[WIKIDATA_ID_KEY].dropna().unique()
    )
    matched_df = matched_df.loc[
        lambda df: ~df[WIKIDATA_ID_KEY].isin(already_used_wikidata_ids)
    ]

    # Sort for Contraint 2 and 3
    matched_df = matched_df.sort_values(
        by=[ARTIST_PRO_SEARCH_SCORE_KEY, "matching_score", "gkg"],
        ascending=False,
    )

    # Constraint 2: if a wikidata_id is matched to multiple artist_ids, keep
    # the match for the artist with the highest pro search score
    matched_df = matched_df.drop_duplicates(subset=[WIKIDATA_ID_KEY], keep="first")

    # Constraint 3: keep the best match per artist_id
    matched_df = matched_df.drop_duplicates(subset=[ARTIST_ID_KEY], keep="first")

    if len(matched_df) == 0:
        return pd.DataFrame(columns=ARTISTS_KEYS)

    logger.info(
        f"Successfully matched {len(matched_df)} unmatched artists to Wikidata."
    )
    matched_artists_df = matched_df.assign(
        **{
            ARTIST_NAME_KEY: lambda df: (
                df.wiki_artist_name.fillna(df[ARTIST_NAME_TO_MATCH_KEY])
                .astype(str)
                .apply(lambda s: s.title())
            ),
        }
    )
    return matched_artists_df.loc[:, ARTISTS_KEYS]


def sanity_checks(
    delta_product_df: pd.DataFrame,
    delta_artist_df: pd.DataFrame,
    delta_artist_alias_df: pd.DataFrame,
    applicative_artist_df: pd.DataFrame,
) -> None:
    """Perform comprehensive sanity checks on delta and applicative dataframes.

    This function validates that the delta dataframes are properly structured for
    a metadata refresh operation and that the data quality meets minimum thresholds.

    Args:
        delta_product_df (pd.DataFrame): Delta product dataframe (should be empty).
        delta_artist_df (pd.DataFrame): Delta artist dataframe with updates.
        delta_artist_alias_df (pd.DataFrame): Delta artist alias dataframe (should be empty).
        applicative_artist_df (pd.DataFrame): Current applicative artist dataframe.

    Raises:
        AssertionError: If any sanity check fails, including:
            - Non-empty dataframes that should be empty for metadata refresh
            - Empty dataframes that should contain data
            - Column mismatch in delta dataframes
            - Mismatch in number of artists with wikidata IDs
            - Significant drop in description or image coverage below 95% threshold
    """
    # TODO: Remove this bypass when dev data is compliant (after wiki_ids are in artist tables)
    if os.environ.get("ENV_SHORT_NAME") == "dev":
        logger.warning("Skipping sanity checks since mocked data is not compliant.")
        return

    SANITY_THRESHOLD = 0.95
    # 1. Minimal sanity checks
    assert (
        len(delta_product_df) == 0
    ), "Delta product dataframe is not empty. It should for metadata refresh."
    assert len(delta_artist_df) > 0, "Delta artist dataframe is empty."
    assert (
        len(delta_artist_alias_df) == 0
    ), "Delta artist alias dataframe is not empty. It should for metadata refresh."
    assert len(applicative_artist_df) > 0, "Applicative artist dataframe is empty."

    # 2. Check columns
    assert set(delta_artist_df.columns) == {
        *ARTISTS_KEYS,
        ACTION_KEY,
        COMMENT_KEY,
    }, "Delta artist dataframe has unexpected columns."
    assert set(delta_product_df.columns) == {
        *PRODUCTS_KEYS,
        ACTION_KEY,
        COMMENT_KEY,
    }, "Delta product dataframe has unexpected columns."
    assert set(delta_artist_alias_df.columns) == {
        *ARTIST_ALIASES_KEYS,
        ACTION_KEY,
        COMMENT_KEY,
    }, "Delta artist alias dataframe has unexpected columns."

    # 3. Check that ARTIST_NAME_KEY and WIKI_ID_KEY are not null
    assert (
        delta_artist_df[ARTIST_NAME_KEY].notna().all()
    ), "Delta artist dataframe has null values in artist_name column."
    assert (
        delta_artist_df[WIKIDATA_ID_KEY].notna().all()
    ), "Delta artist dataframe has null values in wikidata_id column."

    # 4. Check that we have roughly at least same number of descriptions and img
    assert (
        delta_artist_df[ARTIST_DESCRIPTION_KEY].notna().sum()
        >= SANITY_THRESHOLD
        * applicative_artist_df[ARTIST_DESCRIPTION_KEY].notna().sum()
    ), f"Delta artist dataframe has fewer descriptions than applicative artist dataframe with given threshold {SANITY_THRESHOLD}."
    assert (
        delta_artist_df[IMG_KEY].notna().sum()
        >= SANITY_THRESHOLD * applicative_artist_df[IMG_KEY].notna().sum()
    ), f"Delta artist dataframe has fewer images than applicative artist dataframe with given threshold {SANITY_THRESHOLD}."


app = typer.Typer()


@app.command()
def main(
    # Input files
    artist_file_path: str = typer.Option(),
    product_artist_link_filepath: str = typer.Option(),
    product_filepath: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    # Output files
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_artist_alias_file_path: str = typer.Option(),
    output_delta_product_artist_link_file_path: str = typer.Option(),
) -> None:
    """Main function to refresh artist metadata from wikidata.

    This function orchestrates the complete metadata refresh process:
    1. Loads artist, artist alias, and wikidata files
    2. Retrieves wikidata IDs for existing artists
    3. Matches unmatched artists with wikidata to find new matches
    4. Matches artists with wikidata to refresh metadata
    5. Creates delta dataframes for the update operation
    6. Performs sanity checks to ensure data quality
    7. Saves the delta dataframes for downstream processing

    Args:
        artist_file_path (str): Path to the parquet file containing artist data.
        product_artist_link_filepath (str): Optional path to product artist link parquet file.
        product_filepath (str): Optional path to products parquet file.
        wiki_base_path (str): Base path for wikidata files.
        wiki_file_name (str): Name of the wikidata file to load.
        output_delta_artist_file_path (str): Output path for delta artist dataframe.
        output_delta_artist_alias_file_path (str): Output path for delta artist alias dataframe.
        output_delta_product_artist_link_file_path (str): Output path for delta product link dataframe.

    Returns:
        None: Function saves output files and logs progress information.
    """
    # 1. Load data
    logger.info("Loading artist data...")
    applicative_artist_df = pd.read_parquet(artist_file_path).rename(
        columns={
            "wikidata_image_file_url": IMG_KEY,
            "wikidata_id": WIKIDATA_ID_KEY,
        }
    )
    artist_with_wikidata_ids_df = applicative_artist_df.loc[
        lambda df: df[WIKIDATA_ID_KEY].notna()
    ]
    wiki_df = load_wikidata(
        wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name
    ).reset_index(drop=True)
    logger.success("Artist data loaded successfully.")
    logger.info(
        f"Number of artists: {len(applicative_artist_df)}, Number of artists with wikidata ids: {len(artist_with_wikidata_ids_df)}, Number of wikidata entries: {len(wiki_df)}"
    )

    # 2. Match on wikidata to have fresh metadatas for already matched artists
    logger.info("Refreshing artist metadatas from wikidata...")
    refreshed_artists_df = artist_with_wikidata_ids_df.merge(
        wiki_df.drop(columns=["alias", "raw_alias"]).drop_duplicates(),
        how="inner",
        on=WIKIDATA_ID_KEY,
        suffixes=("_old", ""),
    )

    # 3. Match existing unmatched artists on Wikidata
    newly_matched_artists_df = match_unmatched_artists_with_wikidata(
        applicative_artist_df=applicative_artist_df,
        artist_with_wikidata_ids_df=artist_with_wikidata_ids_df,
        product_artist_link_filepath=product_artist_link_filepath,
        product_filepath=product_filepath,
        wiki_df=wiki_df,
    )

    # 4. Refresh statistics
    artists_with_wiki_id = artist_with_wikidata_ids_df[WIKIDATA_ID_KEY].notna().sum()
    artists_matched_in_wiki = refreshed_artists_df[ARTIST_NAME_KEY].notna().sum()
    artists_with_wiki_id_no_match = artists_with_wiki_id - artists_matched_in_wiki
    logger.info(
        f"Artists with wikidata_id: {artists_with_wiki_id}, "
        f"Matched in wikidata: {artists_matched_in_wiki}, "
        f"With wikidata_id but no match: {artists_with_wiki_id_no_match}"
    )
    logger.success("Artist metadatas refreshed successfully.")

    # 5. Build delta artist dataframe
    logger.info("Building delta artist dataframe...")
    delta_product_df, delta_artist_df, delta_artist_alias_df = (
        create_delta_df_for_metadata_refresh(
            refreshed_artists_df=refreshed_artists_df,
            newly_matched_artists_df=newly_matched_artists_df,
        )
    )

    logger.success("Delta artist dataframe built successfully.")
    logger.info(f"Number of artists to update: {len(delta_artist_df)}")

    # 6. Sanity check for consistency
    logger.info("Performing sanity checks...")
    sanity_checks(
        delta_product_df=delta_product_df,
        delta_artist_df=delta_artist_df,
        delta_artist_alias_df=delta_artist_alias_df,
        applicative_artist_df=applicative_artist_df,
    )
    logger.success("Sanity checks passed successfully.")

    # 7. Save files
    logger.info("Saving delta dataframes...")
    logger.info(
        f"Saving delta artist dataframes to {output_delta_artist_file_path}, {output_delta_artist_alias_file_path} and {output_delta_product_artist_link_file_path}."
    )
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    delta_product_df.to_parquet(output_delta_product_artist_link_file_path, index=False)
    delta_artist_alias_df.to_parquet(output_delta_artist_alias_file_path, index=False)
    logger.success("Delta dataframes saved successfully.")


if __name__ == "__main__":
    app()
