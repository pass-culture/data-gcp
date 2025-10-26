"""Evaluation utilities for metapath2vec embeddings."""

from __future__ import annotations

import tempfile
from pathlib import Path

import mlflow
import pandas as pd
from google.cloud import storage
from loguru import logger

from src.utils.recommendation_metrics import compute_evaluation_metrics
from src.utils.retrieval import (
    TABLE_NAME,
    compute_all_scores_lazy,
    generate_predictions_lazy,
    join_retrieval_with_metadata,
    load_and_index_embeddings,
    load_metadata_table,
    sample_test_items_lazy,
)

# Default evaluation configuration
DEFAULT_EVAL_CONFIG = {
    "metadata_columns": ["item_id", "gtl_id", "artist_id"],
    "n_samples": 100,
    "n_retrieved": 1000,
    "k_values": [10, 20, 50, 100],
    "relevance_thresholds": [0.3, 0.4, 0.5, 0.6, 0.7],
    "ground_truth_score": "full_score",
    "force_artist_weight": False,
    "rebuild_index": False,
}


def log_metrics_at_k_csv(
    metrics_df: pd.DataFrame, order_by: list[str] | None = None
) -> None:
    """
    Converts metrics DataFrame into tidy format and logs as csv artifact to MLflow.

    Args:
        metrics_df: DataFrame with columns like
        ['score_col', 'k', 'threshold', 'ndcg', 'recall', 'precision']

    Returns:
        tidy_df: DataFrame with columns
        ['score_col', 'k', 'threshold', '{metric}_at_k', 'value']
    """
    tidy_rows = []

    metric_cols = [
        c for c in metrics_df.columns if c not in ["score_col", "k", "threshold"]
    ]

    for _, row in metrics_df.iterrows():
        for metric_name in metric_cols:
            # Only thresholded metrics should vary per threshold
            threshold_val = (
                row["threshold"] if metric_name in ["recall", "precision"] else None
            )
            tidy_rows.append(
                {
                    "score_col": row["score_col"],
                    "k": row["k"],
                    "threshold": threshold_val,
                    "metric": f"{metric_name}_at_k",
                    "value": float(row[metric_name]),
                }
            )

    tidy_df = pd.DataFrame(tidy_rows).sort_values(by=order_by)

    # Log metrics to MLflow
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "evaluation_metrics.csv"
        tidy_df.to_csv(local_path, index=False)
        mlflow.log_artifact(str(local_path), artifact_path=None)


def save_query_score_details_to_gcs(
    results_df: pd.DataFrame,
    score_details_bucket_folder: str,
) -> None:
    """
    Save detailed query-scores DataFrame to a Parquet file in GCS,
    optionally partitioned by columns (Hive-style).

    Args:
        results_df: DataFrame to save.
        score_details_bucket_folder: GCS folder path, e.g., "gs://my-bucket/folder".
        partition_cols: List of columns to partition by.
    """

    client = storage.Client()
    # Parse bucket and prefix
    if not score_details_bucket_folder.startswith("gs://"):
        raise ValueError("score_details_bucket_folder must start with 'gs://'")
    bucket_name = score_details_bucket_folder.split("/", 1)
    _bucket = client.bucket(bucket_name)
    # Save the full DataFrame as Parquet and publish to GCS
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "query_score_details.parquet"
        results_df.to_parquet(local_path, index=False)

    logger.info(f"Pairwise scores saved to: {score_details_bucket_folder}")


def evaluate_embeddings(
    raw_data_parquet_path: str,
    embedding_parquet_path: str,
    eval_config: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Evaluate embedding quality using retrieval metrics.

    This function:
    1. Loads embeddings into LanceDB
    2. Samples query items
    3. Retrieves top-N similar items for each query
    4. Joins with metadata to compute ground truth scores
    5. Computes GTL, artist, and combined scores
    6. Computes retrieval metrics (NDCG, Recall, Precision)


    Args:
        raw_data_parquet_path: Path to raw metadata parquet folder
        embedding_parquet_path: Path to embeddings parquet file
        eval_config: Optional evaluation configuration dict. If provided, overrides
            defaults. Can contain keys:
                - metadata_columns: list[str]
                - n_samples: int
                - n_retrieved: int
                - k_values: list[int]
                - relevance_thresholds: list[float]
                - ground_truth_score: str
                - table_name: str
                - rebuild_index: bool

    Returns:
        Tuple of (metrics_df, results_df)
    """
    # Merge config with defaults
    config = DEFAULT_EVAL_CONFIG.copy()
    if eval_config is not None:
        config.update(eval_config)

    # Extract config values
    metadata_columns = config["metadata_columns"]
    n_samples = config["n_samples"]
    n_retrieved = config["n_retrieved"]
    k_values = config["k_values"]
    relevance_thresholds = config["relevance_thresholds"]
    ground_truth_score = config["ground_truth_score"]
    force_artist_weight = config["force_artist_weight"]

    rebuild_index = config["rebuild_index"]
    table_name = TABLE_NAME
    logger.info("=" * 80)
    logger.info("EMBEDDING EVALUATION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Raw data: {raw_data_parquet_path}")
    logger.info(f"Embeddings: {embedding_parquet_path}")
    logger.info(f"Configuration: {config}")
    logger.info("=" * 80)

    # ==================================================================================
    # Step 1: Load and index embedding table
    # ==================================================================================
    logger.info("STEP 1: Loading/Opening LanceDB Embedding Table")

    embedding_table = load_and_index_embeddings(
        parquet_path=embedding_parquet_path,
        table_name=table_name,
        rebuild=rebuild_index,
    )

    # ==================================================================================
    # Step 2: Sample query nodes
    # ==================================================================================
    logger.info("STEP 2: Sampling Query Nodes")

    query_node_ids = sample_test_items_lazy(embedding_table, n_samples=n_samples)

    # ==================================================================================
    # Step 3: Generate predictions
    # ==================================================================================
    logger.info("STEP 3: Generating Predictions")

    df_results = generate_predictions_lazy(
        query_node_ids, table=embedding_table, n_retrieved=n_retrieved
    )

    # ==================================================================================
    # Step 4: Join retrieval results with metadata
    # ==================================================================================
    logger.info("STEP 4: Joining Retrieval Results with Metadata")

    unique_node_ids = (
        pd.concat([df_results["query_node_id"], df_results["retrieved_node_id"]])
        .unique()
        .tolist()
    )

    metadata_table = load_metadata_table(
        parquet_path=raw_data_parquet_path,
        filter_field="item_id",
        filter_values=unique_node_ids,
        columns=metadata_columns,
    )

    df_results = join_retrieval_with_metadata(
        retrieval_results=df_results,
        metadata_df=metadata_table,
        right_on="item_id",
    )
    df_results = df_results.where(pd.notna(df_results), None)

    # ==================================================================================
    # Step 5: Compute ALL scores
    # ==================================================================================
    logger.info("STEP 5: Computing Ground Truth Scores")

    df_results = compute_all_scores_lazy(
        augmented_results=df_results,
        table=embedding_table,
        query_node_ids=query_node_ids,
        artist_col_query="query_artist_id",
        artist_col_retrieved="retrieved_artist_id",
        force_artist_weight=force_artist_weight,
    )

    # ==================================================================================
    # Step 6: Compute Embeddings Evaluation Metrics
    # ==================================================================================
    logger.info("STEP 6: Computing Embeddings Evaluation Metrics")

    metrics_df, df_results = compute_evaluation_metrics(
        retrieval_results=df_results,
        k_values=k_values,
        score_cols=ground_truth_score,
        relevancy_thresholds=relevance_thresholds,
    )

    return metrics_df, df_results
