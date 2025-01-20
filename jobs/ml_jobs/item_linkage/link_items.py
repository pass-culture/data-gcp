import multiprocessing as mp
from typing import List, Optional, Tuple

import pandas as pd
import recordlinkage
import typer
from loguru import logger

from constants import MATCHES_REQUIRED, MATCHING_FEATURES, METADATA_FEATURES
from utils.common import read_parquet_files_from_gcs_directory
from utils.gcs_utils import upload_parquet

app = typer.Typer()


def setup_matching() -> recordlinkage.Compare:
    """
    Setup the record linkage comparison model.

    Returns:
        recordlinkage.Compare: Configured comparison model.
    """
    comparator = recordlinkage.Compare()
    for feature_name, comparison_params in MATCHING_FEATURES.items():
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
    features_w_threshold = features.assign(
        offer_name=lambda df: df["offer_name"].apply(
            lambda value: 1
            if value >= MATCHING_FEATURES["offer_name"]["threshold"]
            else 0
        )
    )
    matches = features[
        features_w_threshold.sum(axis=1) >= MATCHES_REQUIRED
    ].reset_index()
    return matches.rename(
        columns={
            "level_0": "index_1",
            "level_1": "index_2",
            "offer_name": "offer_name_score",
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
    return pd.concat(results)


def prepare_tables(
    indexer: recordlinkage.Index,
    linkage_candidates: pd.DataFrame,
    catalog_clean: pd.DataFrame,
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
    item_singletons = (
        linkage_candidates[["item_id_candidate", "candidates_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    item_synchro_retrived = (
        linkage_candidates[["item_id_synchro", "candidates_id", "_distance"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    candidate_links = indexer.index(item_singletons, item_synchro_retrived)
    item_singletons_clean = enriched_items(
        item_singletons, catalog_clean, "item_id_candidate"
    )
    item_synchro_retrived_clean = enriched_items(
        item_synchro_retrived, catalog_clean, "item_id_synchro"
    )
    return candidate_links, item_singletons_clean, item_synchro_retrived_clean


def enriched_items(items_df, catalog, key):
    """
    Enrich items dataframe with catalog data.

    Args:
        items_df (pd.DataFrame): Items dataframe.
        catalog (pd.DataFrame): Catalog dataframe.
        key (str): Key to merge on.

    Returns:
        pd.DataFrame: Enriched items dataframe.
    """
    return items_df.merge(catalog, left_on=key, right_on="item_id", how="left")


def postprocess_matching(matches, item_singletons_clean, item_synchro_clean):
    """
    Postprocess the matching results.

    Args:
        matches (pd.DataFrame): Matched pairs dataframe.
        item_singletons_clean (pd.DataFrame): Cleaned singletons dataframe.
        item_synchro_clean (pd.DataFrame): Cleaned synchro dataframe.

    Returns:
        pd.DataFrame: Final linkage dataframe.
    """

    def rename_columns(df, prefix):
        return df.rename(
            columns={
                "offer_name": f"offer_name_{prefix}",
                "performer": f"performer_{prefix}",
                "offer_description": f"offer_description_{prefix}",
                "offer_subcategory_id": f"offer_subcategory_id_{prefix}",
            }
        ).drop(columns=["item_id", "candidates_id"])

    matches[["index_1", "index_2"]] = matches[["index_1", "index_2"]].astype(str)
    item_singletons_clean = (
        item_singletons_clean.reset_index()
        .rename(columns={"index": "index_1"})
        .astype({"index_1": str})
    )
    item_synchro_clean = (
        item_synchro_clean.reset_index()
        .rename(columns={"index": "index_2"})
        .astype({"index_2": str})
    )

    item_singletons_renamed = rename_columns(item_singletons_clean, "candidates")
    item_synchro_renamed = rename_columns(item_synchro_clean, "synchro")

    linkage = pd.merge(matches, item_synchro_renamed, on="index_2", how="left")
    linkage_full = pd.merge(
        linkage, item_singletons_renamed, on="index_1", how="left"
    ).drop(columns=["index_1", "index_2"])
    filtered_linkage = linkage_full[
        linkage_full["offer_subcategory_id_candidates"]
        == linkage_full["offer_subcategory_id_synchro"]
    ]
    filtered_linkage["link_id"] = filtered_linkage["item_id_synchro"].apply(hash)
    max_scores = filtered_linkage.groupby("item_id_candidate")[
        "offer_name_score"
    ].transform("max")
    linkage_final = (
        filtered_linkage[filtered_linkage["offer_name_score"] == max_scores]
        .rename(columns={"offer_subcategory_id_candidates": "offer_subcategory_id"})
        .drop(columns=["offer_subcategory_id_synchro"])
    )
    return linkage_final


def extract_unmatched_elements(candidates, output):
    """
    Extract unmatched elements from the candidates.

    Args:
        candidates (pd.DataFrame): Candidates dataframe.
        output (pd.DataFrame): Output dataframe.

    Returns:
        pd.DataFrame: Unmatched elements dataframe.
    """
    output_light = output[["item_id_candidate"]].drop_duplicates()
    candidates = candidates.rename(columns={"item_id": "item_id_candidate"})
    merged_df = candidates.merge(
        output_light, on="item_id_candidate", how="left", indicator=True
    )
    unmatched_elements = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    return unmatched_elements[
        [
            "item_id_candidate",
            "offer_name",
            "offer_description",
            "edition",
            "performer",
            "offer_subcategory_id",
        ]
    ].rename(columns={"item_id_candidate": "item_id"})


@app.command()
def main(
    input_sources_path: str = typer.Option(default=...),
    input_candidates_path: str = typer.Option(default=...),
    linkage_candidates_path: str = typer.Option(default=...),
    output_path: str = typer.Option(default=..., help="Output GCS path"),
    unmatched_elements_path: Optional[str] = typer.Option(
        default=None, help="Output GCS path for unmatched elements"
    ),
) -> None:
    """
    Main function to perform record linkage and upload the results to GCS.

    Args:
        input_sources_path (str): Path to the sources data.
        input_candidates_path (str): Path to the candidates data.
        linkage_candidates_path (str): Path to the linkage candidates data.
        output_path (str): Output GCS path.
        unmatched_elements_path (str): Output GCS path for unmatched elements.
    """
    logger.info("Starting item linkage job")
    indexer_per_candidates = recordlinkage.index.Block(on="candidates_id")
    item_synchro = read_parquet_files_from_gcs_directory(
        input_sources_path, columns=METADATA_FEATURES
    )
    logger.info(f"Loaded {len(item_synchro)} items from sources")
    item_singletons = read_parquet_files_from_gcs_directory(
        input_candidates_path, columns=METADATA_FEATURES
    )
    logger.info(f"Loaded {len(item_singletons)} items from candidates")
    catalog_clean = pd.concat([item_synchro, item_singletons]).drop_duplicates()
    logger.info(f"Catalog cleaned: {len(catalog_clean)} items")
    linkage_candidates = read_parquet_files_from_gcs_directory(
        linkage_candidates_path
    ).rename(columns={"item_id": "item_id_candidate"})
    logger.info(f"Loaded {len(linkage_candidates)} linkage candidates")
    candidate_links, item_singletons_clean, item_synchro_retrived_clean = (
        prepare_tables(indexer_per_candidates, linkage_candidates, catalog_clean)
    )
    logger.info(f"Prepared tables for linkage: {len(candidate_links)} candidate links")
    logger.info(f"Prepared tables for linkage: {len(item_singletons_clean)} singletons")
    logger.info(
        f"Prepared tables for linkage: {len(item_synchro_retrived_clean)} item synchro retrived"
    )
    logger.info("Starting record linkage...")
    comparator = setup_matching()
    matches = multiprocess_matching(
        candidate_links, comparator, item_singletons_clean, item_synchro_retrived_clean
    )
    logger.info(f"Found {len(matches)} matches")
    linkage_final = postprocess_matching(
        matches, item_singletons_clean, item_synchro_retrived_clean
    )
    logger.info(f"Final linkage: {len(linkage_final)} linked items ")
    unmatched_elements = extract_unmatched_elements(item_singletons, linkage_final)
    logger.info(f"Unmatched elements: {len(unmatched_elements)}")
    upload_parquet(dataframe=linkage_final, gcs_path=f"{output_path}/data.parquet")
    upload_parquet(
        dataframe=unmatched_elements, gcs_path=f"{unmatched_elements_path}/data.parquet"
    )


if __name__ == "__main__":
    app()
