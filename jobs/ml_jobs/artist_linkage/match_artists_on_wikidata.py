import pandas as pd
import typer

from utils.clustering_utils import (
    get_cluster_to_nickname_dict,
)
from utils.gcs_utils import upload_parquet

app = typer.Typer()


CATEGORY_MAPPING = {
    "SPECTACLE": ["music"],
    "MUSIQUE_LIVE": ["music"],
    "MUSIQUE_ENREGISTREE": ["music"],
    "LIVRE": ["book"],
    "CINEMA": ["movie"],
}
RATIO_WIKI = 0.5


def match_per_category_no_namesakes(
    artists_df: pd.DataFrame,
    wikidata_df: pd.DataFrame,
) -> pd.DataFrame:
    matched_df_list = []
    for pass_category, wiki_category in CATEGORY_MAPPING.items():
        matched_df_list.append(
            pd.merge(
                artists_df.loc[
                    lambda df, pass_category=pass_category: df.offer_category_id
                    == pass_category
                ],
                wikidata_df.loc[
                    lambda df, wiki_category=wiki_category: df.loc[
                        :, wiki_category
                    ].any(axis=1)
                ].loc[lambda df: ~df.alias.duplicated(keep=False)],
                on="alias",
                how="left",
            )
        )
    return pd.concat(matched_df_list)


def match_namesakes_per_category(
    artists_df: pd.DataFrame,
    wikidata_df: pd.DataFrame,
) -> pd.DataFrame:
    matched_df_list = []
    for pass_category, wiki_category in CATEGORY_MAPPING.items():
        wiki_per_category_df = wikidata_df.loc[
            lambda df, wiki_category=wiki_category: df.loc[:, wiki_category].any(axis=1)
        ]
        wiki_per_category_with_duplicates_df = (
            wiki_per_category_df.loc[lambda df: df.alias.duplicated(keep=False)]
            .sort_values(by=["alias", "gkg"], ascending=False)
            .drop_duplicates(subset="alias")
        )
        matched_df_list.append(
            pd.merge(
                artists_df.loc[
                    lambda df, pass_category=pass_category: df.offer_category_id
                    == pass_category
                ],
                wiki_per_category_with_duplicates_df,
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


def get_cluster_to_id_mapping(matched_df: pd.DataFrame) -> dict:
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
        .loc[lambda df: df.ratio > RATIO_WIKI]
        .sort_values(by=["cluster_id", "ratio"], ascending=False)
        .drop_duplicates(subset="cluster_id")
        .set_index("cluster_id")
        .wiki_id.to_dict()
    )


@app.command()
def main(
    artists_file_path: str = typer.Option(),
    wiki_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artists_file_path).reset_index(drop=True)
    wiki_df = pd.read_parquet(wiki_file_path).reset_index(drop=True)

    # 1. Match artists on wikidata for artists with no namesake
    matched_without_namesake_df = match_per_category_no_namesakes(
        artists_df, wiki_df
    ).assign(has_namesake=False)

    # 2. Match artists on wikidata for namesaked artists
    matched_namesakes_df = (
        match_namesakes_per_category(
            artists_df, wiki_df=pd.read_parquet(wiki_file_path).reset_index(drop=True)
        )
        .loc[lambda df: df.wiki_id.notna()]
        .assign(has_namesake=True)
    ).loc[lambda df: ~df.tmp_id.isin(matched_namesakes_df.tmp_id.unique())]

    # 3. Reconciliate the two dataframes
    matched_df = pd.concat(
        [matched_without_namesake_df, matched_namesakes_df]
    ).reset_index(drop=True)

    # Step 2: Map the new cluster_id based on the wiki_id
    cluster_to_wiki_mappings = get_cluster_to_nickname_dict(matched_df)
    output_df = matched_df.assign(
        artist_id=lambda df: df.cluster_id.map(cluster_to_wiki_mappings).fillna(
            df.cluster_id
        ),
        artist_nickname=lambda df: df.cluster_id.map(
            get_cluster_to_nickname_dict(artists_df)
        ),
    )

    upload_parquet(
        dataframe=output_df,
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()
