import pandas as pd
import typer
from loguru import logger

from constants import (
    ACTION_KEY,
    ARTIST_ALIASES_KEYS,
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    ARTISTS_KEYS,
    COMMENT_KEY,
    DESCRIPTION_KEY,
    IMG_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCTS_KEYS,
    WIKI_ID_KEY,
)
from utils.loading import load_wikidata

ALIAS_MERGE_COLUMNS = [
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
]

ARTIST_WIKI_ID_KEY = "artist_wiki_id"


def retrieve_artist_wikidata_id(
    artist_df: pd.DataFrame, artist_alias_df: pd.DataFrame
) -> pd.DataFrame:
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
        .to_frame(name=WIKI_ID_KEY)
    )

    return artist_df.merge(
        artist_id_with_wikidata_ids,
        how="inner",
        on=ARTIST_ID_KEY,
    )


def create_delta_df_for_metadata_refresh(
    refreshed_artists_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    delta_artist_df = refreshed_artists_df.loc[:, ARTISTS_KEYS].assign(
        **{
            ACTION_KEY: "update",
            COMMENT_KEY: "artist with refreshed metadata from wikidata",
        }
    )
    empty_delta_product_artist_link_df = pd.DataFrame(
        columns=PRODUCTS_KEYS
        + [
            ACTION_KEY,
            COMMENT_KEY,
        ]
    )
    empty_delta_artist_alias_df = pd.DataFrame(
        columns=ARTIST_ALIASES_KEYS
        + [
            ACTION_KEY,
            COMMENT_KEY,
        ]
    )
    return (
        empty_delta_product_artist_link_df,
        delta_artist_df,
        empty_delta_artist_alias_df,
    )


def sanity_checks(
    delta_product_df: pd.DataFrame,
    delta_artist_df: pd.DataFrame,
    delta_artist_alias_df: pd.DataFrame,
    applicative_artist_df: pd.DataFrame,
    applicative_artist_alias_df: pd.DataFrame,
) -> None:
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
    assert (
        len(applicative_artist_alias_df) > 0
    ), "Applicative artist alias dataframe is empty."

    # 2. Check columns
    assert set(delta_artist_df.columns) == set(
        ARTISTS_KEYS + [ACTION_KEY, COMMENT_KEY]
    ), "Delta artist dataframe has unexpected columns."
    assert set(delta_product_df.columns) == set(
        PRODUCTS_KEYS + [ACTION_KEY, COMMENT_KEY]
    ), "Delta product dataframe has unexpected columns."
    assert set(delta_artist_alias_df.columns) == set(
        ARTIST_ALIASES_KEYS + [ACTION_KEY, COMMENT_KEY]
    ), "Delta artist alias dataframe has unexpected columns."

    # 3. Check that we have same number of artists with wiki_id in delta as in applicative
    assert (
        len(delta_artist_df)
        == len(applicative_artist_alias_df[ARTIST_WIKI_ID_KEY].dropna().unique())
    ), "Delta artist dataframe and applicative artist dataframes have different number of artists."

    # 4. Check that we have roughly at least same number of descriptions and img
    assert (
        delta_artist_df[DESCRIPTION_KEY].notna().sum()
        >= SANITY_THRESHOLD * applicative_artist_df[DESCRIPTION_KEY].notna().sum()
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
    artist_alias_file_path: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    # Output files
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_artist_alias_file_path: str = typer.Option(),
    output_delta_product_artist_link_file_path: str = typer.Option(),
) -> None:
    # 1. Load data
    logger.info("Loading artist data...")
    applicative_artist_df = pd.read_parquet(artist_file_path).rename(
        columns={
            "artist_description": DESCRIPTION_KEY,
            "wikidata_image_file_url": IMG_KEY,
        }
    )
    applicative_artist_alias_df = pd.read_parquet(artist_alias_file_path)
    wiki_df = load_wikidata(
        wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name
    ).reset_index(drop=True)
    logger.success("Artist data loaded successfully.")
    logger.info(
        f"Number of artists: {len(applicative_artist_df)}, Number of artist aliases: {len(applicative_artist_alias_df)}, Number of wikidata entries: {len(wiki_df)}"
    )

    # 2. Find wiki_id for existing artists from artist_alias table
    # TODO: Remove this step when applicative_artist_table has wikidata ids
    logger.info("Retrieving wikidata ids for existing artists...")
    artist_with_wikidata_ids_df = retrieve_artist_wikidata_id(
        artist_df=applicative_artist_df,
        artist_alias_df=applicative_artist_alias_df,
    )
    logger.success("Wikidata ids retrieved successfully.")

    # 3. Match on wikidata to have fresh metadatas
    logger.info("Refreshing artist metadatas from wikidata...")
    refreshed_artists_df = artist_with_wikidata_ids_df.merge(
        wiki_df.drop(columns=["alias", "raw_alias"]).drop_duplicates(),
        how="left",
        on=WIKI_ID_KEY,
        suffixes=("_old", ""),
    )

    # 4. Refresh statistics
    artists_with_wiki_id = artist_with_wikidata_ids_df[WIKI_ID_KEY].notna().sum()
    artists_matched_in_wiki = refreshed_artists_df[ARTIST_NAME_KEY].notna().sum()
    artists_with_wiki_id_no_match = artists_with_wiki_id - artists_matched_in_wiki
    logger.info(
        f"Artists with wiki_id: {artists_with_wiki_id}, "
        f"Matched in wikidata: {artists_matched_in_wiki}, "
        f"With wiki_id but no match: {artists_with_wiki_id_no_match}"
    )
    logger.success("Artist metadatas refreshed successfully.")

    # 5. Build delta artist dataframe
    logger.info("Building delta artist dataframe...")
    empty_delta_product_artist_link_df, delta_artist_df, empty_delta_artist_alias_df = (
        create_delta_df_for_metadata_refresh(
            refreshed_artists_df=refreshed_artists_df,
        )
    )
    logger.success("Delta artist dataframe built successfully.")
    logger.info(f"Number of artists to update: {len(delta_artist_df)}")

    # 6. Sanity check for consistency
    logger.info("Performing sanity checks...")
    sanity_checks(
        delta_product_df=empty_delta_product_artist_link_df,
        delta_artist_df=delta_artist_df,
        delta_artist_alias_df=empty_delta_artist_alias_df,
        applicative_artist_df=applicative_artist_df,
        applicative_artist_alias_df=applicative_artist_alias_df,
    )
    logger.success("Sanity checks passed successfully.")

    # 7. Save files
    logger.info("Saving delta dataframes...")
    logger.info(
        f"Saving delta artist dataframes to {output_delta_artist_file_path}, {output_delta_artist_alias_file_path} and {output_delta_product_artist_link_file_path}."
    )
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    empty_delta_artist_alias_df.to_parquet(
        output_delta_artist_alias_file_path, index=False
    )
    empty_delta_product_artist_link_df.to_parquet(
        output_delta_product_artist_link_file_path, index=False
    )
    logger.success("Delta dataframes saved successfully.")


if __name__ == "__main__":
    app()
