import multiprocessing as mp
import uuid
from typing import List, Tuple

import networkx as nx
import pandas as pd
import recordlinkage
import typer
from utils.gcs_utils import upload_parquet
from constants import (
    FEATURES,
    MATCHES_REQUIRED,
    UNKNOWN_NAME,
    UNKNOWN_DESCRIPTION,
    UNKNOWN_PERFORMER,
    INITIAL_LINK_ID,
)


app = typer.Typer()


def setup_matching() -> recordlinkage.Compare:
    """
    Setup the record linkage comparison model.

    Returns:
        recordlinkage.Compare: Configured comparison model.
    """
    comparator = recordlinkage.Compare()
    for feature_name, comparison_params in FEATURES.items():
        comparator.string(
            feature_name,
            feature_name,
            method=comparison_params["method"],
            threshold=comparison_params["threshold"],
            label=feature_name,
        )
    return comparator


def get_links(
    candidate_links: pd.MultiIndex,
    comparator: recordlinkage.Compare,
    table_left: pd.DataFrame,
    table_right: pd.DataFrame,
) -> pd.DataFrame:
    """
    Get links between candidate pairs based on the comparison model.
    Candidate_links are the pairs of indices to compare, from table_left and table_right.
    Then we check the matches for each comparison feature.
    If two entries have the required number of matches then they are linked.
    Args:
        candidate_links (pd.MultiIndex): Candidate links.
        comparator (recordlinkage.Compare): Comparison model.
        table_left (pd.DataFrame): Left table for comparison.
        table_right (pd.DataFrame): Right table for comparison.

    Returns:
        pd.DataFrame: Dataframe containing matched pairs.
    """
    features = comparator.compute(candidate_links, table_left, table_right)
    matches = features[features.sum(axis=1) >= MATCHES_REQUIRED].reset_index()
    matches = matches.rename(columns={"level_0": "index_1", "level_1": "index_2"})
    matches["index_1"] = matches["index_1"].apply(lambda x: f"S-{x}")
    matches["index_2"] = matches["index_2"].apply(lambda x: f"C-{x}")
    return matches


def _chunkify(lst: List, n: int) -> List[List]:
    """
    Split a list into n approximately equal chunks.

    Args:
        lst (List): List to be split.
        n (int): Number of chunks.

    Returns:
        List[List]: List of n chunks.
    """
    return [lst[i::n] for i in range(n)]


def multiprocess_matching(
    candidate_links: pd.MultiIndex,
    cpr_cl: recordlinkage.Compare,
    table_left: pd.DataFrame,
    table_right: pd.DataFrame,
) -> pd.DataFrame:
    """
    Perform record linkage using multiple processes.

    Args:
        candidate_links (pd.MultiIndex): Candidate links.
        cpr_cl (recordlinkage.Compare): Comparison model.
        table_left (pd.DataFrame): Left table for comparison.
        table_right (pd.DataFrame): Right table for comparison.

    Returns:
        pd.DataFrame: Dataframe containing matched pairs.
    """
    n_processors = mp.cpu_count()
    chunks = _chunkify(candidate_links, n_processors)
    args = [(chunk, cpr_cl, table_left, table_right) for chunk in chunks]

    with mp.Pool(processes=n_processors) as pool:
        results = pool.starmap(get_links, args)
    matches = pd.concat(results)
    return matches


def link_matches_by_graph(
    df_matches: pd.DataFrame, sources: pd.DataFrame, candidates: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Link matches using graph connected components.

    Args:
        df_matches (pd.DataFrame): Dataframe containing matched pairs.
        sources (pd.DataFrame): Left table for comparison.
        candidates (pd.DataFrame): Right table for comparison.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Updated left and right tables with link IDs.
    """
    linkage_graph = nx.from_pandas_edgelist(
        df_matches, source="index_1", target="index_2", edge_attr=True
    )

    sources = sources.assign(link_id=INITIAL_LINK_ID)
    candidates = candidates.assign(link_id=INITIAL_LINK_ID)

    connected_ids = list(nx.connected_components(linkage_graph))
    for clusters in range(nx.number_connected_components(linkage_graph)):
        link_id = str(uuid.uuid4())[:8]
        for index in connected_ids[clusters]:
            if "S" in index:
                index = int(index[2:])
                sources.at[index, "link_id"] = str(link_id)
            else:
                index = int(index[2:])
                candidates.at[index, "link_id"] = str(link_id)

    candidates = candidates.assign(
        link_count=candidates.groupby("link_id")["link_id"].transform("count")
    )
    item_linkage = candidates.loc[lambda df: df.link_id != "NC"]
    return item_linkage


def format_series(series: pd.Series, na_value: str):
    return series.replace("", None).str.lower().fillna(value=na_value)


def clean_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the catalog dataframe.

    Args:
        catalog (pd.DataFrame): Catalog dataframe.

    Returns:
        pd.DataFrame: Cleaned catalog dataframe.
    """
    catalog = catalog.assign(
        performer=lambda df: df["performer"]
        .replace("", None)
        .str.lower()
        .fillna(value="unkn"),
        offer_name=lambda df: df["offer_name"]
        .replace("", None)
        .str.lower()
        .fillna(value="no_name"),
        offer_description=lambda df: df["offer_description"]
        .replace("", None)
        .str.lower()
        .fillna(value="no_des"),
    )
    return catalog


def prepare_tables(
    indexer: recordlinkage.Index,
    linkage_candidates: pd.DataFrame,
    catalog_clean: pd.DataFrame,
) -> Tuple[pd.MultiIndex, pd.DataFrame, pd.DataFrame]:
    """
    Prepare tables for record linkage.

    Args:
        indexer (recordlinkage.Index): Indexer for candidate generation.
        df (pd.DataFrame): Dataframe containing linkage candidates.
        catalog (pd.DataFrame): Catalog dataframe with item details.

    Returns:
        Tuple[pd.MultiIndex, pd.DataFrame, pd.DataFrame]: Candidate links, cleaned left and right tables.
    """
    sources = (
        linkage_candidates[["item_id", "candidates_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    candidates = (
        linkage_candidates[["item_id_candidate", "candidates_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    candidate_links = indexer.index(sources, candidates)
    sources_clean = enriched_items(sources, catalog_clean, "item_id")
    candidates_clean = enriched_items(
        candidates, catalog_clean, "item_id_candidate"
    ).drop(columns=["item_id_candidate"])
    return candidate_links, sources_clean, candidates_clean


def enriched_items(items_df, catalog, key):
    items_df = items_df.merge(catalog, left_on=key, right_on="item_id", how="left")
    return items_df


@app.command()
def main(
    source_gcs_path: str = typer.Option("metadata/vector", help="GCS bucket path"),
    sources_table_name: str = typer.Option(
        "item_sources_data", help="Catalog table name"
    ),
    candidates_table_name: str = typer.Option(
        "item_candidates_data", help="Catalog table name"
    ),
    input_table_name: str = typer.Option(
        "linkage_candidates_items", help="Input table name"
    ),
    output_table_name: str = typer.Option(
        "linkage_final_items", help="Output table name"
    ),
) -> None:
    """
    Main function to perform record linkage and upload the results to GCS.

    Args:
        source_gcs_path (str): GCS path to the source data.
        catalog_table_name (str): Name of the catalog table.
        input_table_name (str): Name of the input table with linkage candidates.
        output_table_name (str): Name of the output table to save the final linked items.
    """
    indexer_per_candidates = recordlinkage.index.Block(on="candidates_id")
    sources_clean = pd.read_parquet(
        f"{source_gcs_path}/{sources_table_name}",
        columns=["item_id", "performer", "offer_name", "offer_description"],
    ).pipe(clean_catalog)
    candidates_clean = pd.read_parquet(
        f"{source_gcs_path}/{candidates_table_name}",
        columns=["item_id", "performer", "offer_name", "offer_description"],
    ).pipe(clean_catalog)
    catalog_clean = pd.concat([sources_clean, candidates_clean]).drop_duplicates()
    linkage_candidates = pd.read_parquet(
        f"{source_gcs_path}/{input_table_name}.parquet"
    )
    candidate_links, sources_df, candidates_df = prepare_tables(
        indexer_per_candidates, linkage_candidates, catalog_clean
    )

    comparator = setup_matching()

    matches = multiprocess_matching(
        candidate_links, comparator, sources_df, candidates_df
    )

    item_linkage = link_matches_by_graph(matches, sources_df, candidates_df)

    upload_parquet(
        dataframe=item_linkage,
        gcs_path=f"{source_gcs_path}/{output_table_name}.parquet",
    )
    item_linkage.to_gbq("sandbox_prod.item_linkage", if_exists="replace")


if __name__ == "__main__":
    app()
