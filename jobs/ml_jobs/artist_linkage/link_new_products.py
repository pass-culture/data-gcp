import pandas as pd
import typer

from match_artists_on_wikidata import preprocess_artists
from utils.preprocessing_utils import (
    clean_names,
    extract_first_artist,
    format_names,
)

app = typer.Typer()

# Params
ARTIST_NAME_TO_FILTER = {"multi-artistes", "xxx", "compilation", "tbc", "divers"}

# Columns
ARTIST_ID_KEY = "artist_id"
ID_KEY = "id"
NOT_MATCHED_WITH_ARTISTS_KEY = "Not matched with artists"
REMOVED_PRODUCTS_KEY = "Removed products"
MATCHED_WITH_ARTISTS_KEY = "Matched with artists"
MERGE_COLUMNS = ["product_id", "artist_type"]


def load_product_df(product_filepath: str) -> pd.DataFrame:
    return (
        pd.read_parquet(product_filepath)
        .rename(columns={"offer_product_id": "product_id"})
        .astype({"product_id": int})
        .dropna(subset=["artist_name"])
        .loc[lambda df: df.artist_name != ""]
        .drop_duplicates(
            subset=["product_id", "artist_type"]
        )  # To remove offers with the same product_id and artist_type
    )


def get_products_to_remove_and_link_df(
    products_df: pd.DataFrame,
    product_artist_link_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    actual_product_ids = products_df.loc[:, MERGE_COLUMNS].reset_index(drop=True)
    linked_product_ids = product_artist_link_df.loc[:, MERGE_COLUMNS]
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
        MERGE_COLUMNS,
    ]
    products_to_link_df = merged_df.loc[
        lambda df: df._merge == NOT_MATCHED_WITH_ARTISTS_KEY,
        MERGE_COLUMNS,
    ].merge(products_df, how="left", on=MERGE_COLUMNS)

    return products_to_remove_df, products_to_link_df


def preprocess_before_matching(df: pd.DataFrame) -> pd.DataFrame:
    return (
        (df.pipe(clean_names).pipe(extract_first_artist).pipe(format_names))
        .pipe(preprocess_artists)
        .rename(columns={"alias": "artist_name_to_match"})
        .filter(
            [
                "product_id",
                "artist_type",
                "offer_category_id",
                "artist_name",
                "artist_name_to_match",
                "artist_id",
            ],
        )
        .assign(artist_name_to_match=lambda df: df.artist_name_to_match.str.strip())
        .loc[lambda df: ~df.artist_name_to_match.isin(ARTIST_NAME_TO_FILTER)]
    )


def get_index_max_per_category_and_type(alias_df: pd.DataFrame) -> dict:
    return (
        alias_df.loc[lambda df: ~df.artist_id.str.startswith("Q")]
        .drop_duplicates("artist_id")
        .assign(
            id_per_category=lambda df: df.artist_id.str.split("_").str[-1].astype(int),
        )
        .groupby(["offer_category_id", "artist_type"])
        .agg({"id_per_category": lambda s: s.max() + 1})
        .to_dict()
    )["id_per_category"]


def get_new_artists(
    unlinked_products_df: pd.DataFrame, index_max_per_category_and_type: dict
) -> pd.DataFrame:
    def _generate_artist_id(
        group_index: pd.Series,
        offer_category_id: str,
        artist_type: str,
        index_max_per_category_and_type: dict,
    ) -> pd.Series:
        return (
            offer_category_id
            + "_"
            + artist_type
            + "_"
            + (
                group_index
                + index_max_per_category_and_type.get(
                    (offer_category_id, artist_type), 0
                )
            ).astype(str)
        )

    new_artists_id_list = []
    for group_name, group in unlinked_products_df.drop_duplicates(
        ["offer_category_id", "artist_type", "artist_name_to_match"]
    ).groupby(["offer_category_id", "artist_type"], as_index=False):
        offer_category_id = group_name[0]
        artist_type = group_name[1]
        new_artists_id_list.append(
            group.reset_index(drop=True).assign(
                group_index=lambda df: df.index,
                id=lambda df: df.group_index.pipe(
                    _generate_artist_id,
                    offer_category_id=offer_category_id,
                    artist_type=artist_type,
                    index_max_per_category_and_type=index_max_per_category_and_type,
                ),
            )
        )
    return (
        pd.concat(new_artists_id_list)
        .loc[
            :,
            [
                "id",
                "offer_category_id",
                "artist_type",
                "artist_name",
                "artist_name_to_match",
            ],
        ]
        .reset_index(drop=True)
    ).rename(
        columns={
            "artist_name": "name",
            "artist_name_to_match": "name_to_match",
            "artist_type": "type",
        }
    )


@app.command()
def main(
    artist_alias_file_path: str = typer.Option(),
    product_artist_link_filepath: str = typer.Option(),
    product_filepath: str = typer.Option(),
    output_delta_artist_file_path: str = typer.Option(),
    output_delta_artist_alias_file_path: str = typer.Option(),
    output_delta_product_artist_link_filepath: str = typer.Option(),
) -> None:
    alias_df = pd.read_parquet(artist_alias_file_path).dropna(subset=[ARTIST_ID_KEY])
    product_artist_link_df = pd.read_parquet(product_artist_link_filepath).dropna(
        subset=[ARTIST_ID_KEY]
    )
    product_df = load_product_df(product_filepath)

    # %% Split products between to remove and to link
    products_to_remove_df, products_to_link_df = get_products_to_remove_and_link_df(
        product_df, product_artist_link_df
    )

    # %% Preprocess artists before matching
    products_to_link_preproc_df = products_to_link_df.pipe(preprocess_before_matching)
    artist_alias_preproc_df = (
        alias_df.rename(columns={"artist_alias_name": "artist_name"})
        .pipe(preprocess_before_matching)
        .drop(columns=["artist_name"])
        .drop_duplicates()
    )

    # %% Match products with artists
    matched_df = products_to_link_preproc_df.merge(
        artist_alias_preproc_df,
        how="left",
        on=["artist_name_to_match", "artist_type", "offer_category_id"],
    )
    linked_products_df = matched_df.loc[lambda df: df.artist_id.notna()]
    unlinked_products_df = matched_df.loc[lambda df: df.artist_id.isna()]

    # %% 1. Create new artist table
    index_max_per_category_and_type = get_index_max_per_category_and_type(alias_df)
    new_artist_df = get_new_artists(
        unlinked_products_df, index_max_per_category_and_type
    )

    # %% 2.Create new artist alias table
    new_artist_alias_df = (
        unlinked_products_df.merge(
            new_artist_df,
            how="left",
            left_on=["artist_name_to_match", "artist_type", "offer_category_id"],
            right_on=["name_to_match", "type", "offer_category_id"],
        )
        .loc[
            lambda df: df.artist_id.isna(),
            [
                "id",
                "offer_category_id",
                "artist_type",
                "artist_name",
            ],
        ]
        .drop_duplicates()
    )

    # %% 3. Create new product artist link table
    new_artist_product_link_df = (
        unlinked_products_df.drop(columns=["artist_id"])
        .merge(
            new_artist_df.rename(columns={"id": "artist_id"}),
            how="left",
            left_on=["artist_name_to_match", "artist_type", "offer_category_id"],
            right_on=["name_to_match", "type", "offer_category_id"],
        )
        .loc[lambda df: df.artist_id.notna()]
    )

    # %% 4. Create deltas
    PRODUCT_LINK_COLUMNS = ["product_id", "artist_id", "artist_type"]
    delta_product_df = pd.concat(
        [
            linked_products_df.filter(PRODUCT_LINK_COLUMNS).assign(
                action="add", comment="linked to existing artist"
            ),
            new_artist_product_link_df.filter(PRODUCT_LINK_COLUMNS).assign(
                action="add", comment="linked to new artist"
            ),
            products_to_remove_df.filter(PRODUCT_LINK_COLUMNS).assign(
                action="remove", comment="removed linked"
            ),
        ]
    )
    delta_artist_df = new_artist_df.assign(action="add", comment="new artist")
    delta_artist_alias_df = new_artist_alias_df.assign(
        action="add", comment="new artist alias"
    )

    # %% Save files
    delta_artist_df.to_parquet(output_delta_artist_file_path, index=False)
    delta_artist_alias_df.to_parquet(output_delta_artist_alias_file_path, index=False)
    delta_product_df.to_parquet(output_delta_product_artist_link_filepath, index=False)


if __name__ == "__main__":
    app()
