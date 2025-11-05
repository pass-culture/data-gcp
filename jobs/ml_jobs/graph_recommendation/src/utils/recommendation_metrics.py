from typing import Any

import pandas as pd
from loguru import logger
from recommenders.evaluation.python_evaluation import (
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)
from recommenders.utils.constants import (
    DEFAULT_ITEM_COL,
    DEFAULT_PREDICTION_COL,
    DEFAULT_RATING_COL,
    DEFAULT_USER_COL,
)


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
    score_column: str,
    k_values: list[int],
    rating_pred_sorted: pd.DataFrame,
    max_retrieved: int,
) -> pd.DataFrame:
    """
    Compute all metrics for a single score column across all k values and thresholds.
    """
    # Prepare ground truth for this score (done once per score)
    rating_true = _create_ground_truth_dataframe_for_score(df, score_column)

    metrics_per_k = []
    for k in k_values:
        if k > max_retrieved:
            logger.warning(
                f"K={k} exceeds retrieved items ({max_retrieved}). Skipping."
            )
            raise ValueError(f"K={k} exceeds max retrieved items ({max_retrieved})")

        metrics_per_k.append(
            _compute_metrics_at_k(
                rating_true=rating_true,
                rating_pred_sorted=rating_pred_sorted,
                k=k,
                score_column=score_column,
            )
        )
    return pd.DataFrame(metrics_per_k)


def _compute_metrics_at_k(
    rating_true: pd.DataFrame,
    rating_pred_sorted: pd.DataFrame,
    k: int,
    score_column: str,
) -> dict:
    """Compute NDCG, recall, and precision metrics at a specific k value."""
    # Compute NDCG@K (uses raw scores)
    ndcg = ndcg_at_k(
        rating_true=rating_true,
        rating_pred=rating_pred_sorted,
        relevancy_method="top_k",
        k=k,
        score_type="exp",
    )
    logger.info(f"    NDCG@{k}: {ndcg:.4f}")

    # Compute threshold-based metrics (recall, precision)
    rating_pred_k = rating_pred_sorted.groupby("userID").head(k)
    recall = recall_at_k(
        rating_true=rating_true,
        rating_pred=rating_pred_k,
        relevancy_method="top_k",
        k=k,
    )
    custom_recall = custom_recall_at_k(
        rating_true=rating_true,
        rating_pred=rating_pred_k,
        k=k,
    )
    precision = precision_at_k(
        rating_true=rating_true,
        rating_pred=rating_pred_k,
        relevancy_method="top_k",
        k=k,
    )

    return {
        "score_column": score_column,
        "k": k,
        "ndcg": ndcg,
        "recall": recall,
        "custom_recall": custom_recall,
        "precision": precision,
    }


def compute_evaluation_metrics(
    retrieval_results: pd.DataFrame,
    k_values: list[int],
    score_column: str,
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

    logger.info(
        f"Computing metrics for score {score_column}, {len(k_values)} k value(s)"
    )

    # Prepare base dataframe and predictions
    df = _add_prediction_ranks_and_topk_flags(retrieval_results, k_values)
    rating_pred_sorted = _create_sorted_predictions_dataframe(df)
    max_retrieved = df.groupby("query_node_id").size().min()

    # Collect metrics in list for DataFrame
    metrics_df = _compute_all_metrics_for_single_score_column(
        df=df,
        score_column=score_column,
        k_values=k_values,
        rating_pred_sorted=rating_pred_sorted,
        max_retrieved=max_retrieved,
    )

    # Convert to DataFrame
    logger.info("Metrics computation complete")
    logger.info(f"Metrics shape: {metrics_df.shape}")
    return metrics_df, df


#### Custom Recall at K ####
def _get_kth_rating_threshold(
    df: pd.DataFrame,
    k: int,
    col_user: str,
    col_rating: str,
) -> pd.DataFrame:
    """Get the k-th highest rating per user as a threshold."""
    return (
        df.sort_values([col_user, col_rating], ascending=[True, False])
        .groupby(col_user, as_index=False)
        .nth(k - 1)[[col_user, col_rating]]
        .rename(columns={col_rating: "kth_rating"})
    )


def _get_true_topk_with_ties(
    df: pd.DataFrame,
    k: int,
    col_user: str,
    col_item: str,
    col_rating: str,
) -> pd.DataFrame:
    """Get all items with rating >= k-th rating (handles ties)."""
    kth_rating_per_user = _get_kth_rating_threshold(df, k, col_user, col_rating)
    df_with_threshold = df.merge(kth_rating_per_user, on=col_user, how="left")
    return df_with_threshold[
        df_with_threshold[col_rating] >= df_with_threshold["kth_rating"]
    ][[col_user, col_item, col_rating]]


def _get_predicted_topk(
    df: pd.DataFrame,
    k: int,
    col_user: str,
    col_prediction: str,
) -> pd.DataFrame:
    """Get strict top-k predicted items per user."""
    return (
        df.sort_values([col_user, col_prediction], ascending=[True, False])
        .groupby(col_user)
        .head(k)
    )


def _calculate_recall(
    pred_topk: pd.DataFrame,
    true_topk: pd.DataFrame,
    all_users: pd.Index,
    k: int,
    col_user: str,
    col_item: str,
) -> float:
    """Calculate average recall@k across all users."""
    merged = pred_topk.merge(true_topk, on=[col_user, col_item])
    overlap_per_user = merged.groupby(col_user).size()
    overlap_per_user = overlap_per_user.reindex(all_users, fill_value=0)
    return (overlap_per_user / k).mean()


def custom_recall_at_k(
    rating_true: pd.DataFrame,
    rating_pred: pd.DataFrame,
    k: int,
    col_user=DEFAULT_USER_COL,
    col_item=DEFAULT_ITEM_COL,
    col_prediction=DEFAULT_PREDICTION_COL,
    col_rating=DEFAULT_RATING_COL,
) -> float:
    """
    Calculate recall@k with proper handling of tied ratings in ground truth.

    When ground truth ratings have ties, all items with rating >= k-th highest rating
    are included in the relevant set. This prevents arbitrary exclusion of tied items
    and ensures fair evaluation.
    """
    true_topk_with_ties = _get_true_topk_with_ties(
        df=rating_true,
        k=k,
        col_user=col_user,
        col_item=col_item,
        col_rating=col_rating,
    )
    pred_topk = _get_predicted_topk(
        df=rating_pred,
        k=k,
        col_user=col_user,
        col_prediction=col_prediction,
    )
    all_users = pd.concat([rating_true[col_user], rating_pred[col_user]]).unique()
    return _calculate_recall(
        pred_topk, true_topk_with_ties, all_users, k, col_user, col_item
    )
