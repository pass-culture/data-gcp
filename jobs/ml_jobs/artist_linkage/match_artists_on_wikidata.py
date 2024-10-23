import pandas as pd
import typer

from utils.gcs_utils import upload_parquet

app = typer.Typer()


CATEGORY_MAPPING = {
    "SPECTACLE": ["music"],
    "MUSIQUE_LIVE": ["music"],
    "MUSIQUE_ENREGISTREE": ["music"],
    "LIVRE": ["book"],
    "CINEMA": ["movie"],
}


def match_per_category_no_namesakes(
    artists_df: pd.DataFrame,
    wikidata_df: pd.DataFrame,
) -> pd.DataFrame:
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


def normalize_string_series(s: pd.Series) -> pd.Series:
    return (
        s.str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.strip()
        .str.replace(".", "")
    )


def preprocess_artists(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        preprocessed_first_artist=lambda df: normalize_string_series(df.first_artist),
        part_1=lambda df: df.preprocessed_first_artist.str.split(",").str[0],
        part_2=lambda df: df.preprocessed_first_artist.str.split(",").str[1],
        alias=lambda df: df.part_1.where(df.part_2.isna(), df.part_2 + " " + df.part_1),
        tmp_id=lambda df: df.index,
    )


def preprocess_wiki(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(alias=lambda df: df.alias.pipe(normalize_string_series))
        .rename(columns={"artist_name": "wiki_artist_name"})
        .drop_duplicates(subset=["wiki_id", "alias"])
    )


def get_cluster_to_wiki_mapping(matched_df: pd.DataFrame) -> dict:
    """
    Generates a mapping from cluster IDs to wiki IDs based on booking counts.

    Args:
        matched_df (pd.DataFrame): A DataFrame containing the matched data with columns 'cluster_id', 'wiki_id', and 'total_booking_count'.
    Returns:
        dict: A dictionary mapping each cluster ID to the corresponding wiki ID with the highest booking ratio.
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
        .wiki_id.to_dict()
    )


@app.command()
def main(
    linked_artists_file_path: str = typer.Option(),
    wiki_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = (
        pd.read_parquet(linked_artists_file_path)
        .reset_index(drop=True)
        .pipe(preprocess_artists)
    )
    wiki_df = (
        pd.read_parquet(wiki_file_path).reset_index(drop=True).pipe(preprocess_wiki)
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
    matched_df = pd.concat(
        [matched_without_namesake_df, matched_namesakes_df]
    ).reset_index(drop=True)

    # 4. Map the new cluster_id based on the wiki_id
    cluster_to_wiki_mappings = get_cluster_to_wiki_mapping(matched_df)
    output_df = matched_df.assign(
        artist_id=lambda df: df.cluster_id.map(cluster_to_wiki_mappings).fillna(
            df.cluster_id
        ),
        artist_id_name=lambda df: df.wiki_artist_name.fillna(
            df.artist_nickname.str.title()
        ),
    )

    upload_parquet(
        dataframe=output_df,
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()
