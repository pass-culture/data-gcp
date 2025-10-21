"""
Evaluation metrics for graph embeddings using GTL scores as ground truth.
"""

import numpy as np
import pandas as pd
from loguru import logger
from sklearn.metrics import ndcg_score


def compute_precision_at_k(
    retrieval_results: pd.DataFrame,
    k: int,
    relevance_threshold: float = 0.5,
) -> float:
    """
    Precision@k: Fraction of top-k retrieved items that are relevant.

    An item is "relevant" if its GTL score >= relevance_threshold.

    Args:
        retrieval_results: DataFrame with ['query_node_id', 'rank', 'gtl_score']
        k: Cutoff
        relevance_threshold: GTL score threshold for relevance

    Returns:
        Mean precision@k across all queries
    """
    precisions = []

    for query_node in retrieval_results["query_node_id"].unique():
        query_results = retrieval_results[
            (retrieval_results["query_node_id"] == query_node)
            & (retrieval_results["rank"] <= k)
        ]

        if len(query_results) == 0:
            continue

        n_relevant = (query_results["gtl_score"] >= relevance_threshold).sum()
        precision = n_relevant / k  # Divide by k, not len(query_results)
        precisions.append(precision)

    mean_precision = np.mean(precisions) if precisions else 0.0
    logger.debug(
        f"Precision@{k} (threshold={relevance_threshold}): {mean_precision:.4f}"
    )
    return mean_precision


def compute_recall_at_k(
    retrieval_results: pd.DataFrame,
    k: int,
    relevance_threshold: float = 0.5,
    total_relevant_per_query: dict[str, int] | None = None,
) -> float:
    """
    Recall@k: Fraction of all relevant items retrieved in top-k.

    Note: For item-to-item retrieval, it's hard to know the total number of
    relevant items without computing GTL scores for ALL items. Two options:

    1. Assume total_relevant is unknown → compute "recall among retrieved"
    2. Provide total_relevant_per_query if you pre-computed it

    Args:
        retrieval_results: DataFrame with ['query_node_id', 'rank', 'gtl_score']
        k: Cutoff
        relevance_threshold: GTL score threshold
        total_relevant_per_query: Dict mapping query_node_id to total # relevant items

    Returns:
        Mean recall@k
    """
    recalls = []

    for query_node in retrieval_results["query_node_id"].unique():
        query_results = retrieval_results[
            retrieval_results["query_node_id"] == query_node
        ]

        # Top-k results
        topk_results = query_results[query_results["rank"] <= k]
        n_relevant_retrieved = (topk_results["gtl_score"] >= relevance_threshold).sum()

        if total_relevant_per_query is not None:
            # Use provided total
            total_relevant = total_relevant_per_query.get(query_node, 1)
        else:
            # Use ALL retrieved items to estimate total relevant
            total_relevant = (query_results["gtl_score"] >= relevance_threshold).sum()

        if total_relevant == 0:
            continue

        recall = n_relevant_retrieved / total_relevant
        recalls.append(recall)

    mean_recall = np.mean(recalls) if recalls else 0.0
    logger.debug(f"Recall@{k} (threshold={relevance_threshold}): {mean_recall:.4f}")
    return mean_recall


def compute_ndcg_at_k(
    retrieval_results: pd.DataFrame,
    k: int,
) -> float:
    """
    NDCG@k: Normalized Discounted Cumulative Gain.

    Uses GTL scores as continuous relevance labels (not binary).
    Higher GTL scores = more relevant items should be ranked higher.

    Args:
        retrieval_results: DataFrame with ['query_node_id', 'rank', 'gtl_score']
        k: Cutoff

    Returns:
        Mean NDCG@k across all queries
    """
    ndcg_scores = []

    for query_node in retrieval_results["query_node_id"].unique():
        query_results = retrieval_results[
            (retrieval_results["query_node_id"] == query_node)
            & (retrieval_results["rank"] <= k)
        ].sort_values("rank")

        if len(query_results) < 2:
            continue

        # GTL scores are the true relevance
        true_relevance = query_results["gtl_score"].values.reshape(1, -1)

        # Predicted scores (embedding similarities)
        pred_scores = query_results["similarity_score"].values.reshape(1, -1)

        try:
            ndcg = ndcg_score(true_relevance, pred_scores, k=k)
            ndcg_scores.append(ndcg)
        except ValueError:
            # Skip if all relevance scores are 0
            continue

    mean_ndcg = np.mean(ndcg_scores) if ndcg_scores else 0.0
    logger.debug(f"NDCG@{k}: {mean_ndcg:.4f}")
    return mean_ndcg


def compute_mean_reciprocal_rank(
    retrieval_results: pd.DataFrame,
    relevance_threshold: float = 0.5,
) -> float:
    """
    MRR: Mean Reciprocal Rank of the first relevant item.

    Args:
        retrieval_results: DataFrame with ['query_node_id', 'rank', 'gtl_score']
        relevance_threshold: GTL score threshold

    Returns:
        Mean reciprocal rank
    """
    reciprocal_ranks = []

    for query_node in retrieval_results["query_node_id"].unique():
        query_results = retrieval_results[
            retrieval_results["query_node_id"] == query_node
        ].sort_values("rank")

        # Find first relevant item
        relevant_items = query_results[
            query_results["gtl_score"] >= relevance_threshold
        ]

        if len(relevant_items) > 0:
            first_relevant_rank = relevant_items.iloc[0]["rank"]
            reciprocal_ranks.append(1.0 / first_relevant_rank)
        else:
            reciprocal_ranks.append(0.0)

    mrr = np.mean(reciprocal_ranks) if reciprocal_ranks else 0.0
    logger.debug(f"MRR (threshold={relevance_threshold}): {mrr:.4f}")
    return mrr


def aggregate_metrics(
    retrieval_results: pd.DataFrame,
    list_k: list[int],
    relevance_thresholds: list[float] = [0.5, 0.7],
) -> dict[str, float]:
    """
    Compute all evaluation metrics for different k values and thresholds.

    Args:
        retrieval_results: DataFrame with GTL scores computed
        list_k: List of k values
        relevance_thresholds: List of GTL score thresholds for binary relevance

    Returns:
        Dictionary of all metrics
    """
    metrics = {}

    logger.info("Computing evaluation metrics")

    # MRR (not k-specific)
    for threshold in relevance_thresholds:
        mrr = compute_mean_reciprocal_rank(retrieval_results, threshold)
        metrics[f"mrr_threshold_{threshold}"] = mrr

    # Metrics for each k
    for k in list_k:
        logger.info(f"  Computing metrics for k={k}")

        # NDCG (continuous relevance)
        metrics[f"ndcg_at_{k}"] = compute_ndcg_at_k(retrieval_results, k)

        # Precision and Recall for each threshold
        for threshold in relevance_thresholds:
            metrics[f"precision_at_{k}_threshold_{threshold}"] = compute_precision_at_k(
                retrieval_results, k, threshold
            )
            metrics[f"recall_at_{k}_threshold_{threshold}"] = compute_recall_at_k(
                retrieval_results, k, threshold
            )

    return metrics
