import multiprocessing as mp
import re
import string
import unicodedata
from typing import List, Tuple

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


def clean_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the catalog dataframe.

    Args:
        catalog (pd.DataFrame): Catalog dataframe.

    Returns:
        pd.DataFrame: Cleaned catalog dataframe.
    """
    catalog = catalog.assign(
        performer=lambda df: df["performer"].replace("", None).str.lower(),
        offer_name=lambda df: df["offer_name"].replace("", None).str.lower(),
        offer_description=lambda df: df["offer_description"]
        .replace("", None)
        .str.lower(),
    )
    return catalog


def remove_accents(input_str):
    """
    Removes accents from a given string.
    """
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def preprocess_string(s):
    # Lowercasing
    if s is None:
        return s
    s = s.lower()

    # Trimming
    s = s.strip()

    # Removing punctuation and special characters
    s = re.sub(r"[^\w\s]", "", s)

    s = re.sub(f"[{string.punctuation}]", "", s)

    return remove_accents(s)


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
    item_singletons_clean = item_singletons_clean.assign(
        offer_name=lambda df: df["offer_name"].apply(preprocess_string),
    )
    item_synchro_retrived_clean = enriched_items(
        item_synchro_retrived, catalog_clean, "item_id_synchro"
    )
    item_synchro_retrived_clean["offer_name"] = item_synchro_retrived_clean[
        "offer_name"
    ].astype(str)
    item_synchro_retrived_clean = item_synchro_retrived_clean.assign(
        offer_name=lambda df: df["offer_name"].apply(preprocess_string),
    )
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
    linkage_final = pd.merge(
        linkage,
        item_singletons_renamed,
        left_on="index_1",
        right_on="index_1",
        how="left",
    )
    linkage_final = linkage_final.drop(columns=["index_1", "index_2"])
    mask = (
        linkage_final["offer_subcategory_id_candidates"]
        == linkage_final["offer_subcategory_id_synchro"]
    )

    # Filter the DataFrame using the mask
    filtered_linkage_final = linkage_final[mask]
    filtered_linkage_final["link_id"] = filtered_linkage_final["item_id_synchro"].apply(
        hash
    )
    return filtered_linkage_final


@app.command()
def main(
    input_sources_path: str = typer.Option(default=...),
    input_candidates_path: str = typer.Option(default=...),
    linkage_candidates_path: str = typer.Option(default=...),
    output_path: str = typer.Option(default=..., help="Output GCS path"),
) -> None:
    """
    Main function to perform record linkage and upload the results to GCS.

    Args:
        input_sources_path (str): Path to the sources data.
        input_candidates_path (str): Path to the candidates data.
        linkage_candidates_path (str): Path to the linkage candidates data.
        output_path (str): Output GCS path.
    """
    logger.info("Starting item linkage job")
    logger.info("Setup indexer..")
    indexer_per_candidates = recordlinkage.index.Block(on="candidates_id")
    logger.info("Reading data..")
    item_synchro = read_parquet_files_from_gcs_directory(
        input_sources_path,
        columns=[
            "item_id",
            "performer",
            "offer_name",
            "offer_description",
            "offer_subcategory_id",
        ],
    ).pipe(clean_catalog)

    item_singletons = read_parquet_files_from_gcs_directory(
        input_candidates_path,
        columns=[
            "item_id",
            "performer",
            "offer_name",
            "offer_description",
            "offer_subcategory_id",
        ],
    ).pipe(clean_catalog)

    catalog_clean = pd.concat([item_synchro, item_singletons]).drop_duplicates()
    linkage_candidates = pd.read_parquet(linkage_candidates_path)
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
    logger.info("Uploading results..")
    upload_parquet(
        dataframe=linkage_final,
        gcs_path=output_path,
    )


if __name__ == "__main__":
    app()
