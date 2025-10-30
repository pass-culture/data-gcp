from typing import Any

import pandas as pd
from loguru import logger
from recommenders.evaluation.python_evaluation import (
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)


def _convert_thresholds_to_list_and_validate(
    relevancy_thresholds: list[float] | float,
) -> list[float]:
    """Convert thresholds to list format and validate all values are <= 1."""
    if isinstance(relevancy_thresholds, float):
        relevancy_thresholds = [relevancy_thresholds]
    assert all(threshold <= 1 for threshold in relevancy_thresholds)
    return relevancy_thresholds


def _convert_score_cols_to_list(score_cols: list[str] | str) -> list[str]:
    """Convert score columns to list format."""
    if isinstance(score_cols, str):
        score_cols = [score_cols]
    return score_cols


def _add_prediction_ranks_and_topk_flags(
    retrieval_results: pd.DataFrame, k_values: list[int]
) -> pd.DataFrame:
    """Add prediction rank and top-k flag columns to dataframe."""
    df = retrieval_results.copy()
    df["prediction_rank"] = (
        df.groupby("query_node_id")["similarity_score"]
        .rank(ascending=False, method="first")
        .astype(int)
    )
    # Add top-k flags
    for k in k_values:
        df[f"is_in_top_{k}"] = df["prediction_rank"] <= k
    return df


def _create_sorted_predictions_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Create and sort predictions dataframe for use with metrics."""
    rating_pred = df[["query_node_id", "retrieved_node_id", "similarity_score"]].copy()
    rating_pred.columns = ["userID", "itemID", "prediction"]
    # Sort once (reused for all k values)
    rating_pred_sorted = rating_pred.sort_values(
        ["userID", "prediction"], ascending=[True, False]
    )
    return rating_pred_sorted


def _create_ground_truth_dataframe_for_score(
    df: pd.DataFrame, score_col: str
) -> pd.DataFrame:
    """Create ground truth dataframe for a specific score column."""
    rating_true = df[["query_node_id", "retrieved_node_id", score_col]].copy()
    rating_true.columns = ["userID", "itemID", "rating"]
    return rating_true


def _compute_all_metrics_for_single_score_column(
    df: pd.DataFrame,
    score_col: str,
    k_values: list[int],
    relevancy_thresholds: list[float],
    rating_pred_sorted: pd.DataFrame,
    max_retrieved: int,
    metrics_rows: list[dict],
) -> None:
    """
    Compute all metrics for a single score column across all k values and thresholds.
    """
    # Prepare ground truth for this score (done once per score)
    rating_true = _create_ground_truth_dataframe_for_score(df, score_col)

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

        _compute_metrics_at_k(
            rating_true=rating_true,
            rating_pred_sorted=rating_pred_sorted,
            k=k,
            relevancy_thresholds=relevancy_thresholds,
            score_col=score_col,
            metrics_rows=metrics_rows,
        )


def _compute_metrics_at_k(
    rating_true: pd.DataFrame,
    rating_pred_sorted: pd.DataFrame,
    k: int,
    relevancy_thresholds: list[float],
    score_col: str,
    metrics_rows: list[dict],
) -> None:
    """Compute NDCG, recall, and precision metrics at a specific k value."""
    # Compute NDCG@K (uses raw scores)
    try:
        ndcg = ndcg_at_k(
            rating_true=rating_true,
            rating_pred=rating_pred_sorted,
            relevancy_method="top_k",
            k=k,
            score_type="raw",
        )
        logger.info(f"    NDCG@{k}: {ndcg:.4f}")
    except Exception as e:
        logger.error(f"Error computing NDCG @ K={k}: {e}")
        ndcg = None

    # Compute threshold-based metrics (recall, precision)
    rating_pred_k = rating_pred_sorted.groupby("userID").head(k)

    for threshold in relevancy_thresholds:
        try:
            # Recall@K
            recall = recall_at_k(
                rating_true=rating_true,
                rating_pred=rating_pred_k,
                relevancy_method="by_threshold",
                threshold=threshold,
                k=k,
            )
            # Precision@K
            precision = precision_at_k(
                rating_true=rating_true,
                rating_pred=rating_pred_k,
                relevancy_method="by_threshold",
                threshold=threshold,
                k=k,
            )

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
            logger.error(f"Error computing metrics @ K={k}, threshold={threshold}: {e}")
            continue


def compute_evaluation_metrics(
    retrieval_results: pd.DataFrame,
    k_values: list[int],
    relevancy_thresholds: list[float] | float,
    score_cols: list[str] | str = "full_score",
) -> tuple[dict[str, Any], pd.DataFrame]:
    """
    Compute metrics to evaluate embeddings quality (Microsoft Recommenders framework).
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

    # Normalize inputs
    relevancy_thresholds = _convert_thresholds_to_list_and_validate(
        relevancy_thresholds
    )
    score_cols = _convert_score_cols_to_list(score_cols)

    logger.info(
        f"Computing metrics for {len(score_cols)} score(s), "
        f"{len(relevancy_thresholds)} threshold(s), {len(k_values)} k value(s)"
    )

    # Prepare base dataframe and predictions
    df = _add_prediction_ranks_and_topk_flags(retrieval_results, k_values)
    rating_pred_sorted = _create_sorted_predictions_dataframe(df)
    max_retrieved = df.groupby("query_node_id").size().min()

    # Collect metrics in list for DataFrame
    metrics_rows = []

    # Loop: score_col -> k -> threshold
    for score_col in score_cols:
        logger.info(f"Processing score: {score_col}")
        _compute_all_metrics_for_single_score_column(
            df=df,
            score_col=score_col,
            k_values=k_values,
            relevancy_thresholds=relevancy_thresholds,
            rating_pred_sorted=rating_pred_sorted,
            max_retrieved=max_retrieved,
            metrics_rows=metrics_rows,
        )

    # Convert to DataFrame
    metrics_df = pd.DataFrame(metrics_rows)
    logger.info("Metrics computation complete")
    logger.info(f"Metrics shape: {metrics_df.shape}")
    return metrics_df, df
