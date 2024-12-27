import multiprocessing as mp
from typing import List, Optional, Tuple

import pandas as pd
import recordlinkage
import typer
from loguru import logger

from constants import FEATURES, MATCHES_REQUIRED
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
    for feature_name, comparison_params in FEATURES.items():
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

    def threshold(value, thr=0.70):
        if value < thr:
            return 0
        else:
            return 1

    features_w_threshold = features.assign(
        offer_name=lambda df: df["offer_name"].apply(
            threshold, thr=FEATURES["offer_name"]["threshold"]
        ),
    )
    matches = features[
        features_w_threshold.sum(axis=1) >= MATCHES_REQUIRED
    ].reset_index()
    matches = matches.rename(
        columns={
            "level_0": "index_1",
            "level_1": "index_2",
            "offer_name": "offer_name_score",
        }
    )
    matches
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
    item_singletons_clean["offer_name"] = item_singletons_clean["offer_name"].astype(
        str
    )
    # item_singletons_clean = item_singletons_clean.assign(
    #     offer_name=lambda df: df["offer_name"].apply(preprocess_string),
    # )
    item_synchro_retrived_clean = enriched_items(
        item_synchro_retrived, catalog_clean, "item_id_synchro"
    )
    item_synchro_retrived_clean["offer_name"] = item_synchro_retrived_clean[
        "offer_name"
    ].astype(str)
    # item_synchro_retrived_clean = item_synchro_retrived_clean.assign(
    #     offer_name=lambda df: df["offer_name"].apply(preprocess_string),
    # )
    return candidate_links, item_singletons_clean, item_synchro_retrived_clean


def enriched_items(items_df, catalog, key):
    items_df = items_df.merge(catalog, left_on=key, right_on="item_id", how="left")
    return items_df


def postprocess_matching(matches, item_singletons_clean, item_synchro_clean):
    matches["index_1"] = matches["index_1"].astype(str)
    matches["index_2"] = matches["index_2"].astype(str)

    item_singletons_clean = item_singletons_clean.reset_index().rename(
        columns={"index": "index_1"}
    )
    item_singletons_clean["index_1"] = item_singletons_clean["index_1"].astype(str)

    item_synchro_clean = item_synchro_clean.reset_index().rename(
        columns={"index": "index_2"}
    )
    item_synchro_clean["index_2"] = item_synchro_clean["index_2"].astype(str)

    item_singletons_renamed = item_singletons_clean.rename(
        columns={
            "offer_name": "offer_name_candidates",
            "performer": "performer_candidate",
            "offer_description": "offer_description_candidates",
            "offer_subcategory_id": "offer_subcategory_id_candidates",
        }
    ).drop(columns=["item_id", "candidates_id"])

    item_synchro_renamed = item_synchro_clean.rename(
        columns={
            "offer_name": "offer_name_synchro",
            "performer": "performer_synchro",
            "offer_description": "offer_description_synchro",
            "offer_subcategory_id": "offer_subcategory_id_synchro",
        }
    ).drop(columns=["item_id", "candidates_id"])

    linkage = pd.merge(
        matches,
        item_synchro_renamed,
        left_on="index_2",
        right_on="index_2",
        how="left",
    )
    linkage_full = pd.merge(
        linkage,
        item_singletons_renamed,
        left_on="index_1",
        right_on="index_1",
        how="left",
    )
    linkage_full = linkage_full.drop(columns=["index_1", "index_2"])
    mask = (
        linkage_full["offer_subcategory_id_candidates"]
        == linkage_full["offer_subcategory_id_synchro"]
    )

    # Filter the DataFrame using the mask
    filtered_linkage = linkage_full[mask]
    filtered_linkage["link_id"] = filtered_linkage["item_id_synchro"].apply(hash)
    max_scores = filtered_linkage.groupby("item_id_candidate")[
        "offer_name_score"
    ].transform("max")

    # Filter the DataFrame to keep rows where offer_name_score is equal to the maximum score for that item_id_candidate
    linkage_final = filtered_linkage[filtered_linkage["offer_name_score"] == max_scores]
    linkage_final = linkage_final.rename(
        columns={"offer_subcategory_id_candidates": "offer_subcategory_id"}
    ).drop(columns=["offer_subcategory_id_synchro"])
    return linkage_final


def extract_unmatched_elements(candidates, output):
    output_light = output[["item_id_candidate"]].drop_duplicates()
    candidates = candidates.rename(columns={"item_id": "item_id_candidate"})
    logger.info(f"candidates : {candidates.columns}")
    logger.info(f"output_light : {output_light.columns}")
    merged_df = candidates.merge(
        output_light, on="item_id_candidate", how="left", indicator=True
    )

    # Get unmatched offers
    unmatched_elements = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    logger.info(f"Unmatched elements: {unmatched_elements.columns}")
    unmatched_elements_clean = unmatched_elements[
        [
            "item_id_candidate",
            "offer_name",
            "offer_description",
            "edition",
            "performer",
            "offer_subcategory_id",
        ]
    ].rename(
        columns={
            "item_id_candidate": "item_id",
            # "offer_subcategory_id_x": "offer_subcategory_id",
        }
    )
    logger.info(f"Unmatched elements: {unmatched_elements_clean.columns}")
    return unmatched_elements_clean


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
    Main function to perform record linkage and upload the results to GCS.

    Args:
        linkage_type (str): Type of linkage to perform.
        input_sources_path (str): Path to the sources data.
        input_candidates_path (str): Path to the candidates data.
        linkage_candidates_path (str): Path to the linkage candidates data.
        output_path (str): Output GCS path.
        unmatched_elements_path (str): Output GCS path for unmatched elements.
    """
    logger.info("Starting item linkage job")
    logger.info("Setup indexer..")
    indexer_per_candidates = recordlinkage.index.Block(on="candidates_id")
    logger.info("Reading data..")
    load_columns = [
        "item_id",
        "performer",
        "edition",
        "offer_name",
        "offer_description",
        "offer_subcategory_id",
    ]
    if linkage_type == "product":
        item_synchro = read_parquet_files_from_gcs_directory(
            input_sources_path,
            columns=load_columns,
        )
        # .pipe(clean_catalog)

        item_singletons = read_parquet_files_from_gcs_directory(
            input_candidates_path,
            columns=load_columns,
        )
        # .pipe(clean_catalog)
    else:
        item_synchro = pd.read_parquet(
            input_sources_path,
            columns=load_columns,
        )
        # .pipe(clean_catalog)

        item_singletons = pd.read_parquet(
            input_candidates_path,
            columns=load_columns,
        )
        # .pipe(clean_catalog)

    catalog_clean = pd.concat([item_synchro, item_singletons]).drop_duplicates()
    linkage_candidates = pd.read_parquet(linkage_candidates_path).rename(
        columns={"item_id": "item_id_candidate"}
    )
    logger.info("Preparing tables..")
    (
        candidate_links,
        item_singletons_clean,
        item_synchro_retrived_clean,
    ) = prepare_tables(indexer_per_candidates, linkage_candidates, catalog_clean)

    logger.info("Setting up matching..")
    comparator = setup_matching()
    logger.info("Multiprocess matching..")
    matches = multiprocess_matching(
        candidate_links, comparator, item_singletons_clean, item_synchro_retrived_clean
    )
    logger.info("Postprocessing matching..")
    linkage_final = postprocess_matching(
        matches, item_singletons_clean, item_synchro_retrived_clean
    )
    logger.info("Extracting unmatched elements..")
    unmatched_elements = extract_unmatched_elements(item_singletons, linkage_final)
    logger.info("Uploading results..")
    logger.info("Uploading linkage output..")
    upload_parquet(
        dataframe=linkage_final,
        gcs_path=output_path,
    )
    logger.info("Uploading unmatched elements..")
    upload_parquet(
        dataframe=unmatched_elements,
        gcs_path=unmatched_elements_path,
    )


if __name__ == "__main__":
    app()
