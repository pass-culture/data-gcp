import networkx as nx
import pandas as pd
import typer

from src.constants import (
    IMAGE_URL_COLUMN,
    OFFER_ID_COLUMN,
    OFFER_SUBCATEGORY_ID_COL,
)

app = typer.Typer()

# Matching Params
PARTIAL_NAME_SIMILARITY_THRESHOLD = 60
NAME_SIMILARITY_THRESHOLD = 90
DESCRIPTION_SIMILARITY_THRESHOLD = 95
IMAGE_SIMILARITY_THRESHOLD = 0.8
SUBCATECORIES_NOT_MATCHING_ON_OFFER_NAMES = ["SPECTACLE_REPRESENTATION"]


def should_match_on_offer_names(df: pd.DataFrame, subcategory_id: str) -> pd.Series:
    """
    For some subcategories, offer names are not discriminant and can be very similar
        even for different events (e.g. "Spectacle"). In those cases, we only consider
        description and image similarities to determine if two offers match.

    Args:
        df (pd.DataFrame): The dataframe containing the offers to compare.
        subcategory_id(str): The subcategory ID of the offers being compared.
    Returns:
        pd.Series: A boolean series indicating whether the offers match based on
            the selected criteria.
    """
    return (
        (df["description_match"] | df["image_match"])
        if subcategory_id in SUBCATECORIES_NOT_MATCHING_ON_OFFER_NAMES
        else (df["description_match"] | df["name_match"] | df["image_match"]),
    )


def build_cross_df(
    raw_data_df: pd.DataFrame, similarities_df: pd.DataFrame
) -> pd.DataFrame:
    RAW_COLUMNS = [
        OFFER_ID_COLUMN,
        OFFER_SUBCATEGORY_ID_COL,
        IMAGE_URL_COLUMN,
    ]
    selected_df = raw_data_df.loc[:, lambda df: df.columns.isin(RAW_COLUMNS)]

    return similarities_df.merge(
        selected_df.add_suffix("_1"),
        on=f"{OFFER_ID_COLUMN}_1",
        how="left",
    ).merge(
        selected_df.add_suffix("_2"),
        on=f"{OFFER_ID_COLUMN}_2",
        how="left",
    )


@app.command()
def main(
    offer_event_filepath: str = typer.Option(),
    similarities_filepath: str = typer.Option(),
    output_filepath: str = typer.Option(),
) -> None:
    # 1. Load Data
    raw_data_df = pd.read_parquet(offer_event_filepath)
    similarities_df = pd.read_parquet(similarities_filepath)

    # 2. Build cross df with similarities and raw data
    cross_df = build_cross_df(raw_data_df, similarities_df)

    # 3. Clusterize per subcategory
    cluster_dfs = []
    for subcategory in raw_data_df[OFFER_SUBCATEGORY_ID_COL].dropna().unique():
        selected_df = (
            cross_df.loc[lambda df, s=subcategory: df["offer_category_id_1"] == s]
            .reset_index(drop=True)
            .loc[
                lambda df: df["partial_name_similarity"]
                >= PARTIAL_NAME_SIMILARITY_THRESHOLD
            ]
        ).assign(
            description_match=lambda df: (
                df["description_similarity"] >= DESCRIPTION_SIMILARITY_THRESHOLD
            ),
            name_match=lambda df: df["name_similarity"] >= NAME_SIMILARITY_THRESHOLD,
            image_match=lambda df: df["image_similarity"] >= IMAGE_SIMILARITY_THRESHOLD,
            match=lambda df, s=subcategory: should_match_on_offer_names(df, s),
        )

        # Clusterize
        matched_df = selected_df[selected_df["match"]]
        G = nx.from_pandas_edgelist(
            matched_df, source="offer_id_1", target="offer_id_2"
        )
        cluster_df = pd.DataFrame({"cluster": list(nx.connected_components(G))}).assign(
            cluster_length=lambda df: df.cluster.map(len)
        )
        cluster_dfs.append(cluster_df)

    # 4. Merge clusters and save
    pd.concat(cluster_dfs, ignore_index=True).to_parquet(output_filepath, index=False)


if __name__ == "__main__":
    app()
