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
    k_values: list[int],
    relevancy_thresholds: list[float] | float,
    score_cols: list[str] | str = "full_score",
) -> tuple[dict[str, Any], pd.DataFrame]:
    """
    Compute retrieval metrics using Microsoft Recommenders framework.
    Metrics are computed for each query and then averaged

    Evaluation Methods:
        - NDCG@K (Normalized Discounted Cumulative Gain):
            * Measures ranking quality by comparing predicted ranking to ideal ranking
            * Uses raw continuous scores (not binary relevance)
            * Higher scores = better items should be ranked higher
            * Score range: [0, 1], where 1 = perfect ranking
            * Method: relevancy_method=None, score_type="raw"

        - Recall@K:
            * Measures coverage: what fraction of relevant items are in top-K?
            * Formula: (# relevant items in top-K) / (# relevant items total)
            * Uses binary relevance: item is relevant if score >= threshold
            * Score range: [0, 1], where 1 = all relevant items retrieved
            * Method: relevancy_method="by_threshold"

        - Precision@K:
            * Measures accuracy: what fraction of top-K items are relevant?
            * Formula: (# relevant items in top-K) / K
            * Uses binary relevance: item is relevant if score >= threshold
            * Score range: [0, 1], where 1 = all top-K items are relevant
            * Method: relevancy_method="by_threshold"


    Args:
        retrieval_results: DataFrame with columns:
            - query_node_id: The query item (treated as "user")
            - retrieved_node_id: The retrieved item
            - similarity_score: Predicted relevance from embeddings (0-1)
            - score columns: Ground truth relevance scores (0-1)
        k_values: List of K values to evaluate (must be ints).
        score_cols: Column name(s) for ground truth scores. Can be single string or list
            Default: "full_score"
        relevancy_thresholds: Threshold(s) for binary relevance (recall/precision).
            Items with score >= threshold are considered relevant.
            Can be single float or list. Default: 0.9
        training_params: Dict of training parameters to include as columns in output.
            Useful for tracking experiment configurations. Default: {}

    Returns:
        Tuple of (metrics_dataframe, augmented_dataframe):

        metrics_dataframe: Tabular metrics with columns:
            - score_col: Which ground truth score was used
            - k: K value for top-K metrics
            - threshold: Relevance threshold (for recall/precision)
            - ndcg: NDCG@K score [0, 1]
            - recall: Recall@K score [0, 1]
            - precision: Precision@K score [0, 1]

        augmented_dataframe: Input dataframe with added columns:
            - prediction_rank: Rank by similarity score (1 = best)
            - is_in_top_{k}: Boolean flag for each k value
            - is_relevant_{score_col}_at_{threshold}: Boolean relevance flag

    """
    if isinstance(relevancy_thresholds, float):
        relevancy_thresholds = [relevancy_thresholds]

    assert all(threshold <= 1 for threshold in relevancy_thresholds)

    if isinstance(score_cols, str):
        score_cols = [score_cols]

    logger.info(
        f"Computing metrics for {len(score_cols)} score(s), "
        f"{len(relevancy_thresholds)} threshold(s), {len(k_values)} k value(s)"
    )

    # Prepare augmented dataframe
    df = retrieval_results.copy()
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

    # Sort once (reused for all k values)
    rating_pred_sorted = rating_pred.sort_values(
        ["userID", "prediction"], ascending=[True, False]
    )

    # Check max retrieved per query
    max_retrieved = df.groupby("query_node_id").size().min()

    # Collect metrics in list for DataFrame
    metrics_rows = []

    # Loop: score_col -> k -> threshold
    for score_col in score_cols:
        logger.info(f"Processing score: {score_col}")

        # Prepare ground truth for this score (done once per score)
        rating_true = df[["query_node_id", "retrieved_node_id", score_col]].copy()
        rating_true.columns = ["userID", "itemID", "rating"]

        # Add relevance flags to dataframe (once per score/threshold combo)
        for threshold in relevancy_thresholds:
            col_name = f"is_relevant_{score_col}_at_{threshold}"
            df[col_name] = (df[score_col] >= threshold).astype(bool)

            count_col_name = f"n_relevant_{score_col}_at_{threshold}"
            df[count_col_name] = df.groupby("query_node_id")[col_name].transform("sum")

        for k in k_values:
            if k > max_retrieved:
                logger.warning(
                    f"K={k} exceeds retrieved items ({max_retrieved}). Skipping."
                )
                continue

            logger.info(f"  K={k}")

            # Filter predictions to top-k (once per k)
            rating_pred_k = (
                rating_pred_sorted.groupby("userID").head(k).reset_index(drop=True)
            )

            # Compute NDCG@K (no threshold)
            try:
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
                logger.info(f"    NDCG@{k}: {ndcg:.4f}")
            except Exception as e:
                logger.error(f"Error computing NDCG @ K={k}: {e}")
                ndcg = None

            # Compute threshold-based metrics (recall, precision)
            for threshold in relevancy_thresholds:
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

                    logger.info(
                        f"""    Threshold={threshold}:
                        Recall={recall:.4f},Precision={precision:.4f}
                        """
                    )

                    # Add row to metrics
                    metrics_rows.append(
                        {
                            "score_col": score_col,
                            "k": k,
                            "threshold": threshold,
                            "ndcg": ndcg,
                            "recall": recall,
                            "precision": precision,
                        }
                    )

                except Exception as e:
                    logger.error(
                        f"Error computing metrics @ K={k}, threshold={threshold}: {e}"
                    )
                    continue

    # Convert to DataFrame
    metrics_df = pd.DataFrame(metrics_rows)

    logger.info("Metrics computation complete")
    logger.info(f"Metrics shape: {metrics_df.shape}")

    return metrics_df, df
