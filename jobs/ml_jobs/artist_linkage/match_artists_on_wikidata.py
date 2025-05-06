import os
import uuid

import pandas as pd
import typer
from loguru import logger

from utils.gcs_utils import get_last_date_from_bucket

app = typer.Typer()


CATEGORY_MAPPING = {
    "SPECTACLE": ["music"],
    "MUSIQUE_LIVE": ["music"],
    "MUSIQUE_ENREGISTREE": ["music"],
    "LIVRE": ["book"],
    "CINEMA": ["movie"],
}


def load_wikidata(wiki_base_path: str, wiki_file_name: str) -> pd.DataFrame:
    latest_path = os.path.join(
        wiki_base_path, get_last_date_from_bucket(wiki_base_path), wiki_file_name
    )
    logger.info(f"Loading Wikidata from {latest_path}")

    return pd.read_parquet(latest_path)


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


def preprocess_artists(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the artist names in the given DataFrame.
    This function performs the following steps:
    1. Normalizes the 'first_artist' column.
    2. Splits the normalized 'first_artist' into two parts based on the comma.
    3. Creates an 'alias' column by combining the two parts if the second part is not NaN.
    4. Adds a temporary ID column based on the DataFrame's index.
    5. Drops the intermediate columns used for processing.
    Args:
        df (pd.DataFrame): The input DataFrame containing artist names.
    Returns:
        pd.DataFrame: The preprocessed DataFrame with the new 'alias' and 'tmp_id' columns.
    """

    return df.assign(
        part_1=lambda df: df.first_artist.str.split(",").str[0],
        part_2=lambda df: df.first_artist.str.split(",").str[1],
        alias=lambda df: df.part_1.where(
            df.part_2.isna(), df.part_2.astype(str) + " " + df.part_1.astype(str)
        ),
        tmp_id=lambda df: df.index,
    ).drop(columns=["part_1", "part_2"])


def preprocess_wiki(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the given DataFrame by normalizing the 'alias' column, renaming the 'artist_name' column to
    'wiki_artist_name', and dropping duplicate rows based on 'wiki_id' and 'alias' columns.
    Args:
        df (pd.DataFrame): The input DataFrame containing artist data with columns 'alias', 'artist_name', and 'wiki_id'.
    Returns:
        pd.DataFrame: The preprocessed DataFrame with normalized 'alias', renamed 'wiki_artist_name', and duplicates removed.
    """

    return df.rename(columns={"artist_name": "wiki_artist_name"}).drop_duplicates(
        subset=["wiki_id", "alias"]
    )


def get_cluster_to_wiki_mapping(matched_df: pd.DataFrame) -> dict:
    """
    Generates a mapping of cluster IDs to Wikipedia IDs based on booking data.
    Args:
        matched_df (pd.DataFrame): A DataFrame containing columns 'cluster_id', 'wiki_id', and 'total_booking_count'.
    Returns:
        dict: A dictionary where keys are cluster IDs and values are Wikipedia IDs.
    The function performs the following steps:
    1. Adds 1 to the 'total_booking_count' to account for offers with no bookings.
    2. Groups the data by 'cluster_id' and 'wiki_id' and sums the 'total_booking_count'.
    3. Calculates the total booking count for each 'cluster_id'.
    4. Computes the ratio of bookings for each 'wiki_id' within a cluster.
    5. Filters out entries where the ratio is below the WIKI_RATIO_THRESHOLD.
    6. Sorts the data by 'cluster_id' and 'ratio' in descending order.
    7. Drops duplicate 'cluster_id' entries, keeping the one with the highest ratio.
    8. Creates a dictionary mapping 'cluster_id' to 'wiki_id'.
    Note:
        The WIKI_RATIO_THRESHOLD is set to 0.5.
    """

    WIKI_RATIO_THRESHOLD = 0.5

    booking_by_cluster_and_wiki_ids = (
        matched_df.assign(
            total_booking_count=lambda df: df.total_booking_count + 1
        )  # Small hack to still take into account offers even if they have no booking
        .groupby(["cluster_id", "wiki_id"])["total_booking_count"]
        .sum()
    )
    sum_by_cluster_id = booking_by_cluster_and_wiki_ids.groupby("cluster_id").sum()

    return (
        booking_by_cluster_and_wiki_ids.reset_index()
        .assign(
            count_by_cluster=lambda df: df.cluster_id.map(sum_by_cluster_id),
            ratio=lambda df: df.total_booking_count / df.count_by_cluster,
        )
        .loc[lambda df: df.ratio > WIKI_RATIO_THRESHOLD]
        .sort_values(by=["cluster_id", "ratio"], ascending=False)
        .drop_duplicates(subset="cluster_id")
        .set_index("cluster_id")
        .assign(internal_id=lambda df: "MULTI_" + df.wiki_id)
        .internal_id.to_dict()
    )


def get_cluster_to_artist_id_mapping(df: pd.DataFrame) -> dict:
    """
    Generates a mapping from cluster IDs to unique artist IDs.
    Args:
        df (pd.DataFrame): A DataFrame containing a column 'cluster_id' with cluster identifiers.
    Returns:
        dict: A dictionary where keys are cluster IDs and values are unique artist IDs.
    """

    return (
        df.drop_duplicates(subset="cluster_id")
        .assign(artist_id=lambda df: [uuid.uuid4() for _ in range(len(df))])
        .astype({"artist_id": str})  # Convert UUID to string for pyarrow compatibility
        .set_index("cluster_id")
        .artist_id.to_dict()
    )


def get_artist_representative(matched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Determine the representative artist for each artist cluster based on various attributes.
    This function assigns a score to each artist based on the presence of certain attributes
    (wiki_id, img, description, gkg_id, book, music, movie) and the total booking count.
    The artist with the highest score within each artist cluster is flagged as the representative.
    Args:
        df (pd.DataFrame): DataFrame containing artist data with the following columns:
        - artist_id: Identifier for the artist.
        - wiki_id: Wikipedia ID of the artist.
        - img: URL of the artist's image.
        - description: Description of the artist.
        - gkg_id: GKG ID of the artist.
        - book: Boolean indicating if the artist is associated with books.
        - music: Boolean indicating if the artist is associated with music.
        - movie: Boolean indicating if the artist is associated with movies.
        - total_booking_count: Total booking count for the artist.
    Returns:
        pd.DataFrame: DataFrame with an additional column `is_cluster_representative` indicating
                  whether the row is the representative for the artist cluster, and sorted by
                  artist_id and alias_score in descending order.
    """

    scored_df = matched_df.assign(
        alias_score=lambda df: (
            1e3 * (df.wiki_id.notna().astype(float))
            + 1e2 * (df.img.notna().astype(float))
            + 1e1 * (df.description.notna().astype(float))
            + 1e0
            * (
                df.gkg_id.notna().astype(float)
                + df.book.fillna(False).infer_objects(copy=False).astype(float)
                + df.music.fillna(False).infer_objects(copy=False).astype(float)
                + df.movie.fillna(False).infer_objects(copy=False).astype(float)
            )
            + df.total_booking_count / max(1, (df.total_booking_count.max()))
        ).fillna(0.0),
    )

    # Add a flag `is_cluster_representative` stating that the row will be used to create the artist table.
    representative_idx = scored_df.loc[
        scored_df.groupby("artist_id")["alias_score"].idxmax()
    ].index

    return scored_df.assign(
        is_cluster_representative=lambda df: df.index.to_series().apply(
            lambda idx: idx in representative_idx
        )
    ).sort_values(["artist_id", "alias_score"], ascending=[True, False])


@app.command()
def main(
    linked_artists_file_path: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = (
        pd.read_parquet(linked_artists_file_path)
        .reset_index(drop=True)
        .pipe(preprocess_artists)
    )
    wiki_df = (
        load_wikidata(wiki_base_path=wiki_base_path, wiki_file_name=wiki_file_name)
        .reset_index(drop=True)
        .pipe(preprocess_wiki)
    )

    # 1. Match artists on wikidata for namesaked artists
    matched_namesakes_df = (
        match_namesakes_per_category(artists_df, wiki_df)
        .loc[lambda df: df.wiki_id.notna()]
        .assign(has_namesake=True)
    )

    # 2. Match artists on wikidata for artists with no namesake
    matched_without_namesake_df = (
        match_per_category_no_namesakes(artists_df, wiki_df)
        .assign(has_namesake=False)
        .loc[lambda df: ~df.tmp_id.isin(matched_namesakes_df.tmp_id.unique())]
    )

    # 3. Reconciliate the two dataframes
    matched_df = (
        pd.concat([matched_without_namesake_df, matched_namesakes_df])
        .reset_index(drop=True)
        .assign(
            artist_id_name=lambda df: df.wiki_artist_name.fillna(
                df.artist_nickname.str.title()
            ),
        )
    )

    # 4. Map the new cluster_id based on the wiki_id and define new artist_id
    rematched_df = matched_df.assign(
        cluster_id=lambda df: df.cluster_id.map(get_cluster_to_wiki_mapping(df)).fillna(
            df.cluster_id
        ),
        artist_id=lambda df: df.cluster_id.map(get_cluster_to_artist_id_mapping(df)),
    )

    # 5. Add a flag indicating whether the row will be used to build the artist table
    output_df = rematched_df.pipe(get_artist_representative)

    # 6. Upload the output dataframe
    output_df.to_parquet(output_file_path)


if __name__ == "__main__":
    app()
