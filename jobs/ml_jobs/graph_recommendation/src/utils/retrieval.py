from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs
from loguru import logger
from tqdm import tqdm
import pickle

# Import your existing GTL scoring functions
from metrics import get_gtl_score, get_gtl_matching_score

from constants import (
    GCP_PROJECT_ID,
    BIGQUERY_EMBEDDING_DATASET,
    BIGQUERY_EMBEDDING_TABLE,
)

# Configuration
LIST_K = [50, 100, 200, 500]
ITEM_BATCH_SIZE = 50

ITEM_SIZE = 50000


def load_embeddings_from_bq(
    project_id: str,
    dataset: str,
    table_name: str,
) -> Tuple[pd.DataFrame, np.ndarray]:
    """
    Load item embeddings and metadata from BigQuery.

    Uses plural column names: node_ids, gtl_id

    Returns:
        Tuple of (metadata DataFrame, embeddings array)
    """
    query = f"""
    SELECT 
        node_ids,
        gtl_id,
        embeddings
    FROM `{project_id}.{dataset}.{table_name}`
    WHERE embeddings IS NOT NULL AND gtl_ids IS NOT NULL
    LIMIT {ITEM_SIZE}
    """

    logger.info(f"Loading embeddings from BigQuery: {dataset}.{table_name}")
    df = pd.read_gbq(query, project_id=project_id)

    # Handle embeddings RECORD type
    if isinstance(df["embeddings"].iloc[0], dict):
        logger.info("Embeddings stored as RECORD - extracting array")

        # Try common field names
        if "values" in df["embeddings"].iloc[0]:
            embeddings_list = df["embeddings"].apply(lambda x: x["values"]).tolist()
        elif "embedding" in df["embeddings"].iloc[0]:
            embeddings_list = df["embeddings"].apply(lambda x: x["embedding"]).tolist()
        else:
            first_key = list(df["embeddings"].iloc[0].keys())[0]
            logger.warning(f"Using field '{first_key}' from embeddings RECORD")
            embeddings_list = df["embeddings"].apply(lambda x: x[first_key]).tolist()
    elif isinstance(df["embeddings"].iloc[0], (list, np.ndarray)):
        embeddings_list = df["embeddings"].tolist()
    else:
        raise ValueError(
            f"Unexpected embeddings type: {type(df['embeddings'].iloc[0])}"
        )

    # Convert to numpy array
    embeddings = np.array(embeddings_list)

    # Normalize for cosine similarity
    embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

    logger.info(f"Loaded {len(df)} items")
    logger.info(f"Embedding dimension: {embeddings.shape[1]}")

    return df, embeddings


def sample_test_items(
    item_df: pd.DataFrame,
    n_samples: int = 1000,
    # stratify_by: Optional[str] = None,
    random_state: int = 42,
) -> np.ndarray:
    """
    Sample items for evaluation.

    Args:
        item_df: DataFrame with node_ids and gtl_ids
        n_samples: Number of items to sample
        stratify_by: Column to stratify sampling
        random_state: Random seed

    Returns:
        Array of sampled node_ids
    """
    # if stratify_by:
    #     if stratify_by == "gtl_level_1":
    #         # Stratify by top-level GTL (first 2 characters)
    #         item_df["_strata"] = item_df["gtl_ids"].str[:2]
    #         stratify_col = "_strata"
    #     else:
    #         stratify_col = stratify_by

    #     logger.info(f"Using stratified sampling by {stratify_col}")
    #     sampled = item_df.groupby(stratify_col, group_keys=False).apply(
    #         lambda x: x.sample(
    #             n=min(len(x), max(1, n_samples // item_df[stratify_col].nunique())),
    #             random_state=random_state,
    #         )
    #     )

    #     if stratify_by == "gtl_level_1":
    #         sampled = sampled.drop(columns=["_strata"])
    # else:
    #     logger.info("Using random sampling")
    #     sampled = item_df.sample(
    #         n=min(n_samples, len(item_df)), random_state=random_state
    #     )
    logger.info("Using random sampling")
    sampled = item_df.sample(n=min(n_samples, len(item_df)), random_state=random_state)
    logger.info(f"Sampled {len(sampled)} items for evaluation")
    return sampled["node_ids"].values


def build_embedding_index(
    embeddings: np.ndarray,
    node_ids: np.ndarray,
    max_k: int,
    distance_measure: str = "dot_product",
) -> tfrs.layers.factorized_top_k.ScaNN:
    """
    Build ScaNN index for efficient similarity search.

    Args:
        embeddings: Normalized item embeddings
        node_ids: Corresponding node IDs
        max_k: Maximum k for retrieval
        distance_measure: "dot_product" for cosine similarity

    Returns:
        ScaNN index
    """
    embeddings_tf = tf.cast(tf.constant(embeddings), tf.float32)
    node_ids_tf = tf.constant(node_ids.astype(str))

    logger.info(f"Building ScaNN index for {len(node_ids)} items")
    scann = tfrs.layers.factorized_top_k.ScaNN(
        k=max_k + 1,
        distance_measure=distance_measure,
        num_leaves=min(500, len(node_ids) // 10),
        num_leaves_to_search=50,
        training_iterations=12,
        parallelize_batch_searches=True,
        num_reordering_candidates=max_k + 1,
    )
    scann_index = scann.index(
        candidates=embeddings_tf,
        identifiers=node_ids_tf,
    )

    logger.info("ScaNN index built successfully")
    return scann_index


def generate_predictions(
    query_node_ids: np.ndarray,
    item_embeddings: np.ndarray,
    all_node_ids: np.ndarray,
    scann_index,
    k: int,
    batch_size: int = ITEM_BATCH_SIZE,
) -> pd.DataFrame:
    """
    Retrieve top-k similar items using embeddings via ScaNN.

    Args:
        query_node_ids: Items to find similarities for
        item_embeddings: All item embeddings (normalized)
        all_node_ids: All node IDs
        scann_index: Pre-built ScaNN index
        k: Number of similar items to retrieve
        batch_size: Batch size for processing

    Returns:
        DataFrame: ['query_node_id', 'retrieved_node_id', 'similarity_score', 'rank']
    """
    logger.info(f"Retrieving top-{k} similar items for {len(query_node_ids)} queries")

    # Create node_id to index mapping
    node_id_to_idx = {node_id: idx for idx, node_id in enumerate(all_node_ids)}

    results = []
    for batch_start in tqdm(
        range(0, len(query_node_ids), batch_size), desc="Retrieving"
    ):
        batch_items = query_node_ids[batch_start : batch_start + batch_size]

        # Get embeddings for query items
        batch_indices = [node_id_to_idx[node_id] for node_id in batch_items]
        batch_embeddings = item_embeddings[batch_indices]

        # Query ScaNN
        scores, candidates = scann_index(tf.constant(batch_embeddings))

        # Process results
        for query_item, candidate_ids, candidate_scores in zip(
            batch_items, candidates.numpy(), scores.numpy(), strict=True
        ):
            # Convert bytes to string
            candidate_ids_str = [
                c.decode() if isinstance(c, bytes) else str(c) for c in candidate_ids
            ]

            # Filter out self-match
            filtered = [
                (cand_id, score)
                for cand_id, score in zip(
                    candidate_ids_str, candidate_scores, strict=True
                )
                if str(cand_id) != str(query_item)
            ][:k]

            # Add to results with rank
            for rank, (retrieved_id, score) in enumerate(filtered, start=1):
                results.append(
                    {
                        "query_node_id": str(query_item),
                        "retrieved_node_id": str(retrieved_id),
                        "similarity_score": float(score),
                        "rank": rank,
                    }
                )

    df_results = pd.DataFrame(results)
    logger.info(f"Retrieved {len(df_results)} (query, item) pairs")
    return df_results


def compute_gtl_scores_for_retrievals(
    retrieval_results: pd.DataFrame,
    item_metadata: pd.DataFrame,
    use_matching_score: bool = True,
) -> pd.DataFrame:
    """
    Compute GTL scores for all retrieved items (ground truth relevance).

    Args:
        retrieval_results: DataFrame with ['query_node_id', 'retrieved_node_id', ...]
        item_metadata: DataFrame with 'node_ids' and 'gtl_ids'
        use_matching_score: If True, use asymmetric matching score; else symmetric

    Returns:
        retrieval_results with added 'gtl_score' column
    """
    logger.info("Computing GTL scores (ground truth relevance)")

    # Create GTL ID lookup
    gtl_lookup = item_metadata.set_index("node_ids")["gtl_ids"].to_dict()

    gtl_scores = []
    for _, row in tqdm(
        retrieval_results.iterrows(), total=len(retrieval_results), desc="GTL scoring"
    ):
        query_gtl = gtl_lookup.get(row["query_node_id"])
        retrieved_gtl = gtl_lookup.get(row["retrieved_node_id"])

        if query_gtl is None or retrieved_gtl is None:
            gtl_scores.append(0.0)
            continue

        # Use your existing GTL scoring functions
        if use_matching_score:
            score = get_gtl_matching_score(query_gtl, retrieved_gtl)
        else:
            score = get_gtl_score(query_gtl, retrieved_gtl)

        gtl_scores.append(score)

    retrieval_results["gtl_score"] = gtl_scores

    logger.info(f"GTL scores computed:")
    logger.info(f"  Mean: {np.mean(gtl_scores):.3f}")
    logger.info(f"  Median: {np.median(gtl_scores):.3f}")
    logger.info(f"  Min: {np.min(gtl_scores):.3f}")
    logger.info(f"  Max: {np.max(gtl_scores):.3f}")

    return retrieval_results


if __name__ == "__main__":
    K = 10
    print("loading_embeddings from BQ")
    df, embeddings = load_embeddings_from_bq(
        GCP_PROJECT_ID, BIGQUERY_EMBEDDING_DATASET, BIGQUERY_EMBEDDING_TABLE
    )
    node_ids = df["node_ids"].unique().dropna()

    print("building scaNN index")
    scann_index = build_embedding_index(
        embeddings, node_ids=node_ids, max_k=50, distance_measure="dot_product"
    )
    print("sampling query nodes")
    query_node_ids = sample_test_items(df, n_samples=100)
    print("generate predictions")
    df_results = generate_predictions(
        query_node_ids, embeddings, node_ids, scann_index, K
    )
    print("computing evaluation metrics")
    retrieval_results = compute_gtl_scores_for_retrievals(
        df_results, df, use_matching_score=True
    )
    print("saving metrics")
    pickle.dumps(retrieval_results, "results/retrieval_results_matching_score.pkl")
    print("saved successfully")
