import multiprocessing as mp
import uuid
from typing import List, Tuple

import networkx as nx
import pandas as pd
import recordlinkage
import typer
from utils.gcs_utils import upload_parquet

# Constants
FEATURES = {
    "offer_name": {"method": "jarowinkler", "threshold": 0.95},
    "offer_description": {"method": "jarowinkler", "threshold": 0.95},
    "performer": {"method": "jarowinkler", "threshold": 0.95},
}
MATCHES_REQUIRED = 3

app = typer.Typer()


# TODO: underscore on methods if not used elsewhere


def setup_matching() -> recordlinkage.Compare:
    """
    Setup the record linkage comparison model.

    Returns:
        recordlinkage.Compare: Configured comparison model.
    """
    comparator = recordlinkage.Compare()
    for feature, feature_dict in FEATURES.items():
        comparator.string(
            feature,
            feature,
            method=feature_dict["method"],
            threshold=feature_dict["threshold"],
            label=feature,
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

    Args:
        candidate_links (pd.MultiIndex): Candidate links.
        comparator (recordlinkage.Compare): Comparison model.
        table_left (pd.DataFrame): Left table for comparison.
        table_right (pd.DataFrame): Right table for comparison.

    Returns:
        pd.DataFrame: Dataframe containing matched pairs.
    """
    performer = (lambda df: df["performer"].fillna(value="unkn"),)
    # TODO: chainer
    features = comparator.compute(candidate_links, table_left, table_right)
    matches = features[features.sum(axis=1) >= MATCHES_REQUIRED].reset_index()
    matches = matches.rename(columns={"level_0": "index_1", "level_1": "index_2"})
    # matches.assign(
    #     index_1=lambda df: f"L-{df['index_1']}",
    #     index_2=lambda df: f"R-{df['index_2']}",
    # )
    matches["index_1"] = matches["index_1"].apply(lambda x: f"source-{x}")
    matches["index_2"] = matches["index_2"].apply(lambda x: f"candidate-{x}")
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

    sources=sources.assign(link_id="NC")
    candidates=candidates.assign(link_id="NC")

    # Todo: repasser dessus car un peu bizarre de devoir relinker et boucle dans les boucles peut être lent
    connected_ids = list(nx.connected_components(linkage_graph))
    for clusters in range(nx.number_connected_components(linkage_graph)):
        link_id = str(uuid.uuid4())[:8]
        for index in connected_ids[clusters]:
            if "source" in index:
                index = int(index[2:])
                sources.at[index, "link_id"] = str(link_id)
            else:
                index = int(index[2:])
                candidates.at[index, "link_id"] = str(link_id)

    candidates = candidates.assign(
        link_count=candidates.groupby("link_id")["link_id"].transform("count")
    )
    return sources,candidates


def clean_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the catalog dataframe.

    Args:
        catalog (pd.DataFrame): Catalog dataframe.

    Returns:
        pd.DataFrame: Cleaned catalog dataframe.
    """
    catalog = catalog.assign(
        performer=lambda df: df["performer"].replace('', None).str.lower().fillna(value="unkn"),
        offer_name=lambda df: df["offer_name"].replace('', None).str.lower().fillna(value="no_name"),
        offer_description=lambda df: df["offer_description"].replace('', None).str.lower().fillna(value="no_des"),
    )
    return catalog


def prepare_tables(
    indexer: recordlinkage.Index,
    df: pd.DataFrame,
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
    # TODO: Chainer et peuit être splitter pour plus de claireté
    sources = df[["item_id", "batch_id"]].drop_duplicates().reset_index(drop=True)
    candidates = (
        df[["item_id_candidate", "batch_id"]].drop_duplicates().reset_index(drop=True)
    )
    candidate_links = indexer.index(sources, candidates)
    return candidate_links, sources, candidates


def enriched_items(items_df, catalog, key):
    items_df = items_df.merge(catalog, left_on=key, right_on="item_id", how="left")
    return items_df


@app.command()
def main(
    source_gcs_path: str = typer.Option("metadata/vector", help="GCS bucket path"),
    catalog_table_name: str = typer.Option("item_data", help="Catalog table name"),
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
    indexer = recordlinkage.index.Block(on="batch_id")
    catalog_clean = pd.read_parquet(
        f"{source_gcs_path}/{catalog_table_name}",
        columns=["item_id", "performer", "offer_name", "offer_description"],
    ).pipe(clean_catalog)
    # catalog_clean=clean_catalog(catalog)
    linkage_candidates = pd.read_parquet(
        f"{source_gcs_path}/{input_table_name}.parquet"
    )
    candidate_links, sources, candidates = prepare_tables(indexer, linkage_candidates)

    comparator = setup_matching()
    sources_clean = enriched_items(sources, catalog_clean, "item_id")
    candidates_clean = enriched_items(
        candidates, catalog_clean, "item_id_candidate"
    ).drop(columns=["item_id_candidate"])

    matches = multiprocess_matching(
        candidate_links, comparator, sources_clean, candidates_clean
    )

    # Todo: gérer le double output
    sources_final, candidates_final = link_matches_by_graph(
        matches, sources_clean, candidates_clean
    )
    candidates_final_clean=candidates_final.query("link_id != 'NC'")
    upload_parquet(
        dataframe=sources_final,
        gcs_path=f"{source_gcs_path}/{output_table_name}_sources.parquet",
    )
    upload_parquet(
        dataframe=candidates_final_clean,
        gcs_path=f"{source_gcs_path}/{output_table_name}_candidates.parquet",
    )


if __name__ == "__main__":
    app()
