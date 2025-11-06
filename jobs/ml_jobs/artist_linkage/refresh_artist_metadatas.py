# %%

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
    DESCRIPTION_KEY,
    IMG_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
    WIKI_ID_KEY,
)
from utils.loading import load_wikidata

ALIAS_MERGE_COLUMNS = [
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
]


def retrieve_artist_wikidata_id(
    artist_df: pd.DataFrame, artist_alias_df: pd.DataFrame
) -> pd.DataFrame:
    artist_id_with_wikidata_ids = (
        artist_alias_df.groupby(ARTIST_ID_KEY)
        .agg(
            tmp_artist_wikidata_id_list=(
                "artist_wiki_id",
                lambda x: x.dropna().unique().tolist(),
            )
        )
        .assign(
            wiki_id=lambda df: df.tmp_artist_wikidata_id_list.apply(
                lambda s: s[0] if len(s) > 0 else None
            ),
        )
        .drop(columns=["tmp_artist_wikidata_id_list"])
    )
    return artist_df.merge(
        artist_id_with_wikidata_ids,
        how="inner",
        on=ARTIST_ID_KEY,
        validate="one_to_one",
    )


def create_delta_df_for_metadata_refresh(
    refreshed_artists_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    delta_artist_df = (
        refreshed_artists_df.assign(
            artist_name=lambda df: df.artist_name_wiki.where(
                df.artist_name_wiki.notna(), df.artist_name
            ),
            description=lambda df: df.description_wiki.where(
                df.description_wiki.notna(), df.description
            ),
            img=lambda df: df.img_wiki.where(df.img_wiki.notna(), df.img),
        )
        .loc[:, [ARTIST_ID_KEY, ARTIST_NAME_KEY, DESCRIPTION_KEY, IMG_KEY]]
        .assign(
            **{
                ACTION_KEY: "update",
                COMMENT_KEY: "artist with refreshed metadata from wikidata",
            }
        )
    )
    empty_delta_product_artist_link_df = pd.DataFrame(
        columns=[
            PRODUCT_ID_KEY,
            ARTIST_ID_KEY,
            ARTIST_TYPE_KEY,
            ACTION_KEY,
            COMMENT_KEY,
        ]
    )
    empty_delta_artist_alias_df = pd.DataFrame(
        columns=[
            ARTIST_ID_KEY,
            OFFER_CATEGORY_ID_KEY,
            ARTIST_TYPE_KEY,
            ARTIST_NAME_KEY,
            ARTIST_NAME_TO_MATCH_KEY,
            WIKI_ID_KEY,
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

    # 2. Check that we have same number of artists in delta as in applicative
    assert (
        len(delta_artist_df) == len(applicative_artist_df)
    ), "Delta artist dataframe and applicative artist dataframe have different number of artists."

    # 3. Check that we have roughly at least same number of descriptions
    assert (
        delta_artist_df[DESCRIPTION_KEY].notna().sum()
        >= SANITY_THRESHOLD * applicative_artist_df[DESCRIPTION_KEY].notna().sum()
    ), f"Delta artist dataframe has fewer descriptions than applicative artist dataframe with given threshold {SANITY_THRESHOLD}."


# %%
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
        on="wiki_id",
        suffixes=("", "_wiki"),
    )
    logger.success("Artist metadatas refreshed successfully.")

    # 4. Build delta artist dataframe
    logger.info("Building delta artist dataframe...")
    empty_delta_product_artist_link_df, delta_artist_df, empty_delta_artist_alias_df = (
        create_delta_df_for_metadata_refresh(
            refreshed_artists_df=refreshed_artists_df,
        )
    )
    logger.success("Delta artist dataframe built successfully.")
    logger.info(f"Number of artists to update: {len(delta_artist_df)}")

    # 5. Sanity check for consistency
    logger.info("Performing sanity checks...")
    sanity_checks(
        delta_product_df=empty_delta_product_artist_link_df,
        delta_artist_df=delta_artist_df,
        delta_artist_alias_df=empty_delta_artist_alias_df,
        applicative_artist_df=applicative_artist_df,
        applicative_artist_alias_df=applicative_artist_alias_df,
    )
    logger.success("Sanity checks passed successfully.")

    # 6. Save files
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
