# Input files
import uuid

import pandas as pd
import typer
from loguru import logger

from src.constants import (
    ACTION_KEY,
    ARTIST_NAME_KEY,
    ARTIST_NAME_TO_MATCH_KEY,
    ARTIST_TYPE_KEY,
    COMMENT_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
)
from src.utils.loading import load_wikidata
from src.utils.matching import create_artists_tables, match_artists_with_wikidata
from src.utils.preprocessing_utils import (
    filter_products,
    prepare_artist_names_for_matching,
)

app = typer.Typer()


def sanity_checks(
    product_artist_link_df: pd.DataFrame,
    artist_df: pd.DataFrame,
    artist_alias_df: pd.DataFrame,
) -> None:
    # 1. Product Artist Links
    unmatched_products = product_artist_link_df.loc[lambda df: df.artist_id.isna()]
    if len(unmatched_products) > 0:
        logger.error("There are products with no artist_id after matching.")
        logger.error(unmatched_products)
        raise ValueError(
            "There are still products that could not be linked to artists after matching."
        )
    assert (
        not product_artist_link_df.drop(columns=[ACTION_KEY, COMMENT_KEY])
        .duplicated()
        .any()
    ), "Duplicate entries in product_artist_link_df"

    # 2. Artists
    assert (
        not artist_df.drop(columns=[ACTION_KEY, COMMENT_KEY]).duplicated().any()
    ), "Duplicate entries in artist_df"

    # 3. Artist Aliases
    assert (
        not artist_alias_df.drop(columns=[ACTION_KEY, COMMENT_KEY]).duplicated().any()
    ), "Duplicate entries in artist_alias_df"

    # Additional checks can be added as needed


@app.command()
def main(
    # Input files
    product_filepath: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    # Output files
    output_artist_file_path: str = typer.Option(),
    output_artist_alias_file_path: str = typer.Option(),
    output_product_artist_link_filepath: str = typer.Option(),
) -> None:
    # 1. Load data
    product_df = (
        pd.read_parquet(product_filepath)
        .astype({PRODUCT_ID_KEY: int})
        .pipe(filter_products)
    )
    wiki_df = load_wikidata(
        wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name
    ).reset_index(drop=True)

    # 2. Match products to link with artists on both raw and preprocessed offer names
    preproc_products_df = product_df.pipe(prepare_artist_names_for_matching)

    # 3. Create new artist clusters by offer_category and artist type
    new_artist_clusters_df = (
        preproc_products_df.groupby(
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

    # 4. Match new artist clusters with existing artists on Wikidata
    exploded_artist_alias_df = match_artists_with_wikidata(
        new_artist_clusters_df=new_artist_clusters_df,
        wiki_df=wiki_df,
    )

    # 5. Create new artists and artist aliases
    product_artist_link_df, artist_df, artist_alias_df = create_artists_tables(
        preproc_unlinked_products_df=preproc_products_df,
        exploded_artist_alias_df=exploded_artist_alias_df,
    )

    # 6. Sanity check for consistency
    sanity_checks(
        product_artist_link_df,
        artist_df,
        artist_alias_df,
    )

    # 7. Save files
    artist_df.to_parquet(output_artist_file_path, index=False)
    artist_alias_df.to_parquet(output_artist_alias_file_path, index=False)
    product_artist_link_df.to_parquet(output_product_artist_link_filepath, index=False)


if __name__ == "__main__":
    app()
