from typing import Any

import pandas as pd
from loguru import logger
from recommenders.evaluation.python_evaluation import (
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)


def compute_retrieval_metrics(
    retrieval_results: pd.DataFrame,
    k_values: list[int] | None = None,
    score_cols: list[str] | str = "full_score",
    relevancy_thresholds: list[float] | float = 0.9,
    training_params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """
    Compute retrieval metrics using Microsoft Recommenders framework.

    Args:
        retrieval_results: DataFrame with columns:
            - query_node_id: The query item (treated as "user")
            - retrieved_node_id: The retrieved item
            - similarity_score: Predicted relevance from embeddings (0-1)
            - score columns: Ground truth relevance scores (0-1)
        k_values: List of K values to evaluate (must be ints)
        score_cols: List of column names for ground truth scores
        relevancy_thresholds: List of thresholds for recall/precision
        training_params: Dict of training parameters to include in output

    Returns:
        Tuple of (metrics_dict, augmented_dataframe)
    """
    if isinstance(relevancy_thresholds, float):
        relevancy_thresholds = [relevancy_thresholds]

    assert all(threshold <= 1 for threshold in relevancy_thresholds)

    if k_values is None:
        k_values = [10, 20, 50, 100]
    if isinstance(score_cols, str):
        score_cols = [score_cols]

    if training_params is None:
        training_params = {}

    logger.info(
        f"Computing metrics for {len(score_cols)} score(s), "
        f"{len(relevancy_thresholds)} threshold(s), {len(k_values)} k value(s)"
    )

    # Prepare augmented dataframe
    df = retrieval_results.copy()

    # Add prediction rank (by similarity score)
    df["prediction_rank"] = (
        df.groupby("query_node_id")["similarity_score"]
        .rank(ascending=False, method="first")
        .astype(int)
    )

    # Add top-k flags
    for k in k_values:
        df[f"is_in_top_{k}"] = df["prediction_rank"] <= k

    # Prepare predictions once (used by all metrics)
    rating_pred = df[["query_node_id", "retrieved_node_id", "similarity_score"]].copy()
    rating_pred.columns = ["userID", "itemID", "prediction"]

    # Initialize metrics structure
    metrics_output = {
        "training_params": training_params,
        "recall": {},
        "precision": {},
        "ndcg": {},
    }

    # Check max retrieved per query
    max_retrieved = df.groupby("query_node_id").size().min()

    # Loop: score_col -> threshold -> k (most efficient for caching)
    for score_col in score_cols:
        logger.info(f"Processing score: {score_col}")

        # Prepare ground truth for this score (done once per score)
        rating_true = df[["query_node_id", "retrieved_node_id", score_col]].copy()
        rating_true.columns = ["userID", "itemID", "rating"]

        # Initialize lists for this score
        metrics_output["recall"][score_col] = []
        metrics_output["precision"][score_col] = []

        # NDCG doesn't use threshold - compute once per score
        ndcg_results = {"K_values": [], "metric_values": []}

        for k in k_values:
            if k > max_retrieved:
                logger.warning(
                    f"K={k} exceeds retrieved items ({max_retrieved}). Skipping."
                )
                continue

            # Filter predictions to top-k (cached across thresholds)
            rating_pred_k = (
                rating_pred.sort_values(
                    ["userID", "prediction"], ascending=[True, False]
                )
                .groupby("userID")
                .head(k)
                .reset_index(drop=True)
            )

            try:
                # NDCG@K (no threshold dependency)
                ndcg = ndcg_at_k(
                    rating_true=rating_true,
                    rating_pred=rating_pred_k,
                    col_user="userID",
                    col_item="itemID",
                    col_rating="rating",
                    col_prediction="prediction",
                    relevancy_method=None,
                    k=k,
                    score_type="raw",
                )
                ndcg_results["K_values"].append(k)
                ndcg_results["metric_values"].append(ndcg)
                logger.info(f"  {score_col} NDCG@{k}: {ndcg:.4f}")

            except Exception as e:
                logger.error(f"Error computing NDCG @ K={k}: {e}")

        metrics_output["ndcg"][score_col] = ndcg_results

        # Threshold-based metrics (recall, precision)
        for threshold in relevancy_thresholds:
            logger.info(f"  Threshold: {threshold}")

            # Add relevance flags to dataframe
            col_name = f"is_relevant_{score_col}_at_{threshold}"
            df[col_name] = (df[score_col] >= threshold).astype(bool)

            # Count relevant items per query
            count_col_name = f"n_relevant_{score_col}_at_{threshold}"
            df[count_col_name] = df.groupby("query_node_id")[col_name].transform("sum")

            # Initialize results for this threshold
            recall_results = {
                "relevancy_thresh": threshold,
                "K_values": [],
                "metric_values": [],
            }
            precision_results = {
                "relevancy_thresh": threshold,
                "K_values": [],
                "metric_values": [],
            }

            for k in k_values:
                if k > max_retrieved:
                    continue

                # Filter predictions to top-k
                rating_pred_k = (
                    rating_pred.sort_values(
                        ["userID", "prediction"], ascending=[True, False]
                    )
                    .groupby("userID")
                    .head(k)
                    .reset_index(drop=True)
                )

                try:
                    # Recall@K
                    recall = recall_at_k(
                        rating_true=rating_true,
                        rating_pred=rating_pred_k,
                        col_user="userID",
                        col_item="itemID",
                        col_rating="rating",
                        col_prediction="prediction",
                        relevancy_method="by_threshold",
                        threshold=threshold,
                        k=k,
                    )
                    recall_results["K_values"].append(k)
                    recall_results["metric_values"].append(recall)

                    # Precision@K
                    precision = precision_at_k(
                        rating_true=rating_true,
                        rating_pred=rating_pred_k,
                        col_user="userID",
                        col_item="itemID",
                        col_rating="rating",
                        col_prediction="prediction",
                        relevancy_method="by_threshold",
                        threshold=threshold,
                        k=k,
                    )
                    precision_results["K_values"].append(k)
                    precision_results["metric_values"].append(precision)

                    logger.info(
                        f"    K={k}: Recall={recall:.4f}, Precision={precision:.4f}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error computing metrics @ K={k}, threshold={threshold}: {e}"
                    )
                    continue

            # Add results for this threshold
            metrics_output["recall"][score_col].append(recall_results)
            metrics_output["precision"][score_col].append(precision_results)

    logger.info("Metrics computation complete")
    return metrics_output, df
