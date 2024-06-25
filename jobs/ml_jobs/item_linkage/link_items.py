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
    # TODO: chainer
    features = comparator.compute(candidate_links, table_left, table_right)
    matches = features[features.sum(axis=1) >= MATCHES_REQUIRED].reset_index()
    matches = matches.rename(columns={"level_0": "index_1", "level_1": "index_2"})
    matches["index_1"] = matches["index_1"].apply(lambda x: f"L-{x}")
    matches["index_2"] = matches["index_2"].apply(lambda x: f"R-{x}")
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
    df_matches: pd.DataFrame, table_left: pd.DataFrame, table_right: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Link matches using graph connected components.

    Args:
        df_matches (pd.DataFrame): Dataframe containing matched pairs.
        table_left (pd.DataFrame): Left table for comparison.
        table_right (pd.DataFrame): Right table for comparison.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Updated left and right tables with link IDs.
    """
    linkage_graph = nx.from_pandas_edgelist(
        df_matches, source="index_1", target="index_2", edge_attr=True
    )

    table_left["link_id"] = ["NC"] * len(table_left)
    table_right["link_id"] = ["NC"] * len(table_right)

    # Todo: repasser dessus car un peu bizarre de devoir relinker et boucle dans les boucles peut être lent
    connected_ids = list(nx.connected_components(linkage_graph))
    for clusters in range(nx.number_connected_components(linkage_graph)):
        link_id = str(uuid.uuid4())[:8]
        for index in connected_ids[clusters]:
            if "L-" in index:
                index = int(index[2:])
                table_left.at[index, "link_id"] = str(link_id)
            else:
                index = int(index[2:])
                table_right.at[index, "link_id"] = str(link_id)

    table_right = table_right.assign(
        link_count=table_right.groupby("link_id")["link_id"].transform("count")
    )
    return table_right, table_left


def prepare_tables(
    indexer: recordlinkage.Index, df: pd.DataFrame, catalog: pd.DataFrame
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
    catalog["performer"] = catalog["performer"].fillna(value="unkn")
    catalog["offer_name"] = catalog["offer_name"].str.lower()
    catalog["offer_description"] = catalog["offer_description"].str.lower()
    table_left = df[["item_id", "batch_id"]].drop_duplicates().reset_index(drop=True)
    table_right = (
        df[["link_item_id", "batch_id"]].drop_duplicates().reset_index(drop=True)
    )

    candidate_links = indexer.index(table_left, table_right)

    table_left_clean = pd.merge(table_left, catalog, on=["item_id"], how="left")
    table_right_clean = pd.merge(
        table_right, catalog, left_on=["link_item_id"], right_on=["item_id"], how="left"
    ).drop(columns=["link_item_id"])
    return candidate_links, table_left_clean, table_right_clean


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
    catalog = pd.read_parquet(
        f"{source_gcs_path}/{catalog_table_name}",
        columns=["item_id", "performer", "offer_name", "offer_description"],
    )
    linkage_candidates = pd.read_parquet(
        f"{source_gcs_path}/{input_table_name}.parquet"
    )

    candidate_links, table_left_clean, table_right_clean = prepare_tables(
        indexer, linkage_candidates, catalog
    )

    comparator = setup_matching()
    matches = multiprocess_matching(
        candidate_links, comparator, table_left_clean, table_right_clean
    )

    # Todo: gérer le double output
    table_left_final, table_right_final = link_matches_by_graph(
        matches, table_left_clean, table_right_clean
    )

    upload_parquet(
        dataframe=table_right_final,
        gcs_path=f"{source_gcs_path}/{output_table_name}_right.parquet",
    )
    upload_parquet(
        dataframe=table_left_final,
        gcs_path=f"{source_gcs_path}/{output_table_name}_left.parquet",
    )


if __name__ == "__main__":
    app()
