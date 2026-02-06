import multiprocessing as mp
import os
from typing import List, Optional, Tuple

import mlflow
import pandas as pd
import recordlinkage
import typer
from loguru import logger

from constants import (
    EXPERIMENT_NAME,
    MATCHES_REQUIRED,
    MATCHING_FEATURES,
    METADATA_FEATURES,
    MLFLOW_RUN_ID_FILENAME,
    RUN_NAME,
)
from utils.common import read_parquet_files_from_gcs_directory
from utils.gcs_utils import upload_parquet
from utils.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment

app = typer.Typer()


def setup_matching(linkage_type: str) -> recordlinkage.Compare:
    """
    Setup the record linkage comparison model.

    Returns:
        recordlinkage.Compare: Configured comparison model.
    """
    comparator = recordlinkage.Compare()
    for feature_name, comparison_params in MATCHING_FEATURES[linkage_type].items():
        if comparison_params["method"] == "exact":
            comparator.exact(
                feature_name,
                feature_name,
                missing_value=comparison_params["missing_value"],
                label=feature_name,
            )
        else:
            comparator.string(
                feature_name,
                feature_name,
                method=comparison_params["method"],
                threshold=None,
                missing_value=comparison_params["missing_value"],
                label=feature_name,
            )

    return comparator


def get_links(
    candidate_links: pd.MultiIndex,
    comparator: recordlinkage.Compare,
    linkage_type: str,
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

    def threshold(value, thr=0.70):
        if value < thr:
            return 0
        else:
            return 1

    features = comparator.compute(candidate_links, table_left, table_right)
    features_w_threshold = features.assign(
        oeuvre=lambda df: df["oeuvre"].apply(
            threshold, thr=MATCHING_FEATURES[linkage_type]["oeuvre"]["threshold"]
        )
    )
    matches = features[
        features_w_threshold.sum(axis=1) >= MATCHES_REQUIRED
    ].reset_index()
    return matches.rename(
        columns={
            "level_0": "index_1",
            "level_1": "index_2",
            "oeuvre": "oeuvre_score",
        }
    )


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
    linkage_type: str,
    table_left: pd.DataFrame,
    table_right: pd.DataFrame,
) -> pd.DataFrame:
    """
    Perform record linkage using multiple processes.

    Args:
        candidate_links (pd.MultiIndex): Candidate links.
        cpr_cl (recordlinkage.Compare): Comparison model.
        linkage_type (str): Type of linkage to perform.
        table_left (pd.DataFrame): Left table for comparison.
        table_right (pd.DataFrame): Right table for comparison.

    Returns:
        pd.DataFrame: Dataframe containing matched pairs.
    """
    n_processors = mp.cpu_count()
    chunks = _chunkify(candidate_links, n_processors)
    args = [(chunk, cpr_cl, linkage_type, table_left, table_right) for chunk in chunks]
    with mp.Pool(processes=n_processors) as pool:
        results = pool.starmap(get_links, args)
    return pd.concat(results)


def prepare_links_and_extract_ids(
    indexer: recordlinkage.Index,
    linkage_candidates: pd.DataFrame,
) -> Tuple[pd.MultiIndex, pd.DataFrame, pd.DataFrame]:
    """
    Prepare tables for record linkage.

    Args:
        indexer (recordlinkage.Index): Indexer for candidate generation.
        linkage_candidates (pd.DataFrame): Dataframe containing linkage candidates.
        catalog (pd.DataFrame): Catalog dataframe with item details.

    Returns:
        Tuple[pd.MultiIndex, pd.DataFrame, pd.DataFrame]: Candidate links, cleaned left and right tables.
    """

    candidates_linked_ids = (
        linkage_candidates[["item_id_candidate", "candidates_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"item_id_candidate": "item_id"})
    )
    sources_retrived_ids = (
        linkage_candidates[["item_id_synchro", "candidates_id", "_distance"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"item_id_synchro": "item_id"})
    )
    links_candidate_source = indexer.index(candidates_linked_ids, sources_retrived_ids)

    return links_candidate_source, candidates_linked_ids, sources_retrived_ids


def postprocess_matching(matches, item_singletons_clean, sources_clean):
    """
    Postprocess the matching results.

    Args:
        matches (pd.DataFrame): Matched pairs dataframe.
        item_singletons_clean (pd.DataFrame): Cleaned singletons dataframe.
        sources_clean (pd.DataFrame): Cleaned synchro dataframe.

    Returns:
        pd.DataFrame: Final linkage dataframe.
    """

    item_singletons_clean = (
        item_singletons_clean.reset_index()
        .rename(columns={"index": "index_1"})
        .astype({"index_1": str})
    )

    sources_clean = (
        sources_clean.reset_index()
        .rename(columns={"index": "index_2"})
        .astype({"index_2": str})
    )

    matches[["index_1", "index_2"]] = matches[["index_1", "index_2"]].astype(str)

    linkage_raw = (
        matches.merge(sources_clean, on="index_2", how="left")
        .merge(
            item_singletons_clean,
            on="index_1",
            how="left",
            suffixes=("_synchro", "_candidate"),
        )
        .drop(columns=["index_1", "index_2"])
    )

    max_scores = linkage_raw.groupby("item_id_candidate")["oeuvre_score"].transform(
        "max"
    )
    logger.info(f"linkage_raw columns: {linkage_raw.columns}")
    linkage_clean = linkage_raw[linkage_raw["oeuvre_score"] == max_scores]
    logger.info(f"linkage_clean columns: {linkage_clean.columns}")
    num_duplicate_matches = (
        linkage_clean["item_id_candidate"].value_counts().gt(1).sum()
    )
    linkage_final = linkage_clean.drop_duplicates(
        subset=["item_id_synchro", "item_id_candidate"]
    )
    logger.info(f"linkage_final columns: {linkage_final.columns}")
    return linkage_final, num_duplicate_matches


def extract_unmatched_elements(
    candidates: pd.DataFrame, output: pd.DataFrame
) -> pd.DataFrame:
    """
    Extract unmatched elements from the candidates.

    Args:
        candidates (pd.DataFrame): Candidates dataframe.
        output (pd.DataFrame): Output dataframe.

    Returns:
        pd.DataFrame: Unmatched elements dataframe.
    """
    item_id_candidates = output[["item_id_candidate"]].drop_duplicates()
    merged_df = (
        candidates[["item_id"]]
        .merge(
            item_id_candidates,
            left_on="item_id",
            right_on="item_id_candidate",
            how="left",
            indicator=True,
        )
        .drop(columns=["item_id_candidate"])
    )
    unmatched_elements = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    return unmatched_elements


@app.command()
def main(
    linkage_type: str = typer.Option(default=..., help="Type of linkage to perform"),
    input_sources_path: str = typer.Option(default=...),
    input_candidates_path: str = typer.Option(default=...),
    linkage_candidates_path: str = typer.Option(default=...),
    output_path: str = typer.Option(default=..., help="Output GCS path"),
    unmatched_elements_path: Optional[str] = typer.Option(
        default=None, help="Output GCS path for unmatched elements"
    ),
) -> None:
    """
    Perform record linkage and upload the results to GCS.

    This function:
      1) Reads the sources data from the specified path.
      2) Reads the candidates data from the specified path.
      3) Reads the existing linkage candidates.
      4) Performs record linkage using configured block and compare operations.
      5) Identifies any unmatched records and optionally saves them.
      6) Writes the final linkage output to the specified GCS path.

    Args:
        linkage_type (str): Type of linkage to perform.
        input_sources_path (str): Path to the sources data.
        input_candidates_path (str): Path to the candidates data.
        linkage_candidates_path (str): Path to the linkage candidates data.
        output_path (str): The GCS path to save the final linkage results.
        unmatched_elements_path (Optional[str]): Optional path to save the unmatched elements.
    """
    logger.info("Starting item linkage job")
    indexer_per_candidates = recordlinkage.index.Block(on="candidates_id")

    sources = read_parquet_files_from_gcs_directory(
        input_sources_path, columns=METADATA_FEATURES
    )
    logger.info(f"Loaded {len(sources)} items from sources")
    candidates = read_parquet_files_from_gcs_directory(
        input_candidates_path, columns=METADATA_FEATURES
    )
    logger.info(f"Loaded {len(candidates)} items from candidates")
    catalog = pd.concat([sources, candidates]).drop_duplicates()
    logger.info(f"catalog: {len(catalog)} items")
    logger.info(f"catalog columns: {catalog.columns}")
    linkage_candidates = read_parquet_files_from_gcs_directory(linkage_candidates_path)
    logger.info(
        f"Loaded {len(linkage_candidates)} linkage candidates with columns: {linkage_candidates.columns}"
    )
    links_candidate_source, candidates_linked_ids, sources_retrived_ids = (
        prepare_links_and_extract_ids(indexer_per_candidates, linkage_candidates)
    )

    candidates_linked_clean = candidates_linked_ids.merge(
        catalog, on="item_id", how="left"
    )
    sources_retrived_clean = sources_retrived_ids.merge(
        catalog, on="item_id", how="left"
    )
    logger.info(
        f"Prepared tables for linkage: {len(links_candidate_source)} candidate links"
    )
    logger.info(f"candidates_linked_clean.columns: {candidates_linked_clean.columns}")
    logger.info(
        f"Prepared tables for linkage: {candidates_linked_clean.item_id.nunique() } distinct candidates"
    )
    logger.info(
        f"Prepared tables for linkage: {sources_retrived_clean.item_id.nunique()} distinct item synchro retrived"
    )
    logger.info("Starting record linkage...")
    comparator = setup_matching(linkage_type)
    matches = multiprocess_matching(
        links_candidate_source,
        comparator,
        linkage_type,
        candidates_linked_clean,
        sources_retrived_clean,
    )
    logger.info(f"Found {len(matches)} matches")
    linkage_final, num_duplicate_matches = postprocess_matching(
        matches, candidates_linked_clean, sources_retrived_clean
    )
    logger.info(f"Final linkage: {len(linkage_final)} linked items ")
    unmatched_elements = extract_unmatched_elements(candidates, linkage_final)
    logger.info(f"Unmatched elements: {len(unmatched_elements)}")

    connect_remote_mlflow()
    experiment = get_mlflow_experiment(EXPERIMENT_NAME)
    run_id_file = f"{MLFLOW_RUN_ID_FILENAME}.txt"
    if os.path.exists(run_id_file):
        with open(run_id_file, mode="r") as file:
            run_id = file.read()
    else:
        run_id = None
    with mlflow.start_run(
        experiment_id=experiment.experiment_id, run_name=RUN_NAME, run_id=run_id
    ):
        run_id = mlflow.active_run().info.run_id
        with open(f"{MLFLOW_RUN_ID_FILENAME}.txt", mode="w") as file:
            file.write(run_id)
        mlflow.log_params(
            params={
                f"sources_ {linkage_type}_count": len(sources),
                f"candidates_ {linkage_type}_count": len(candidates),
                f"linkage_{linkage_type}_count": len(linkage_final),
                f"duplicated_matches_{linkage_type}_count": num_duplicate_matches,
            }
        )
    upload_parquet(dataframe=linkage_final, gcs_path=f"{output_path}/data.parquet")
    upload_parquet(
        dataframe=unmatched_elements, gcs_path=f"{unmatched_elements_path}/data.parquet"
    )


if __name__ == "__main__":
    app()
