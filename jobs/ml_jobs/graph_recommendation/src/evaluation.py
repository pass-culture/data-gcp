"""Evaluation utilities for metapath2vec embeddings."""

from __future__ import annotations

import pandas as pd
from loguru import logger

from src.constants import DefaultEvaluationConfig
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


def evaluate_embeddings(
    raw_data_parquet_path: str,
    embedding_parquet_path: str,
    eval_config: DefaultEvaluationConfig | dict | None = None,
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
    if eval_config is None:
        config = DefaultEvaluationConfig()
    elif isinstance(eval_config, DefaultEvaluationConfig):
        config = eval_config
    elif isinstance(eval_config, dict):
        config = DefaultEvaluationConfig()
        config.update_from_dict(eval_config)
    else:
        logger.warning(
            "eval_config must be DefaultEvaluationConfig, dict, or None; using defaults"
        )
        config = DefaultEvaluationConfig()

    # Extract config values
    metadata_columns = config.metadata_columns
    n_samples = config.n_samples
    n_retrieved = config.n_retrieved
    k_values = config.k_values
    relevance_thresholds = config.relevance_thresholds
    ground_truth_score = config.ground_truth_score
    force_artist_weight = config.force_artist_weight
    rebuild_index = config.rebuild_index

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
