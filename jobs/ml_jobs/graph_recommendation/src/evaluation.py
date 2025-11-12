"""Evaluation utilities for metapath2vec embeddings."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

from src.constants import FULL_SCORE_COLUMN, ID_COLUMN
from src.utils.preprocessing import preprocess_metadata_dataframe
from src.utils.recommendation_metrics import compute_evaluation_metrics
from src.utils.retrieval import (
    LANCEDB_TABLE_NAME,
    compute_all_scores,
    generate_predictions_lazy,
    join_retrieval_with_metadata,
    load_and_index_embeddings,
    sample_test_items_lazy,
)

if TYPE_CHECKING:
    from src.config import EvaluationConfig


def evaluate_embeddings(
    raw_data_parquet_path: str,
    embedding_parquet_path: str,
    evaluation_config: EvaluationConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("=" * 80)
    logger.info("EMBEDDING EVALUATION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Raw data: {raw_data_parquet_path}")
    logger.info(f"Embeddings: {embedding_parquet_path}")
    logger.info(f"Configuration: {evaluation_config.to_dict()}")
    logger.info("=" * 80)

    # ==================================================================================
    # Step 1: Load and index embedding table
    # ==================================================================================
    logger.info("STEP 1: Loading/Opening LanceDB Embedding Table")

    embedding_table = load_and_index_embeddings(
        parquet_path=embedding_parquet_path,
        table_name=LANCEDB_TABLE_NAME,
        rebuild=evaluation_config.rebuild_index,
    )

    # ==================================================================================
    # Step 2: Sample query nodes
    # ==================================================================================
    logger.info("STEP 2: Sampling Query Nodes")

    query_node_ids = sample_test_items_lazy(
        embedding_table, n_samples=evaluation_config.n_samples
    )

    # ==================================================================================
    # Step 3: Generate predictions
    # ==================================================================================
    logger.info("STEP 3: Generating Predictions")

    df_results = generate_predictions_lazy(
        query_node_ids, table=embedding_table, n_retrieved=evaluation_config.n_retrieved
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
    metadata_df = (
        pd.read_parquet(
            raw_data_parquet_path,
            columns=[ID_COLUMN, *evaluation_config.metadata_columns],
        )
        .loc[lambda df: df[ID_COLUMN].isin(unique_node_ids)]
        .drop_duplicates(subset=[ID_COLUMN])
        .pipe(
            preprocess_metadata_dataframe,
            metadata_columns=evaluation_config.metadata_columns,
        )
    )
    df_results = join_retrieval_with_metadata(
        retrieval_results=df_results,
        metadata_df=metadata_df,
        right_on=ID_COLUMN,
    )

    # ==================================================================================
    # Step 5: Compute ALL scores
    # ==================================================================================
    logger.info("STEP 5: Computing Ground Truth Scores")

    df_results = compute_all_scores(
        retrieved_results_with_metadata_df=df_results,
        metadata_columns=evaluation_config.metadata_columns,
        categorical_metadata_columns=evaluation_config.categorical_metadata_columns,
        hierarchical_metadata_scoring_functions=evaluation_config.hierarchical_metadata_scoring_functions,
    )

    # ==================================================================================
    # Step 6: Compute Embeddings Evaluation Metrics
    # ==================================================================================
    logger.info("STEP 6: Computing Embeddings Evaluation Metrics")
    metrics_df, df_results = compute_evaluation_metrics(
        retrieval_results=df_results,
        k_values=evaluation_config.k_values,
        score_column=FULL_SCORE_COLUMN,
    )

    return metrics_df, df_results
