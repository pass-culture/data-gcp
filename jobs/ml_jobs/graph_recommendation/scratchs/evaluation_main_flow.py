import pandas as pd
from loguru import logger

from src.constants import ENV_SHORT_NAME, ML_BUCKET_TEMP
from src.utils.recommendation_metrics import (
    compute_all_scores_lazy,
    compute_retrieval_metrics,
    generate_predictions_lazy,
    join_retrieval_with_metadata,
    load_and_index_embeddings,
    load_metadata_table,
    sample_test_items_lazy,
)

if __name__ == "__main__":
    N_RETRIEVED = 1000
    K_VALUES_TO_EVAL = [10, 20, 50, 100]
    TABLE_NAME = "embedding_table"
    RUN_ID = "algo_training_graph_embeddings_20251021T070003"
    PARQUET_PATH = f"gs://{ML_BUCKET_TEMP}/algo_training_{ENV_SHORT_NAME}/{RUN_ID}/embeddings.parquet"

    # Metadata configuration
    METADATA_PARQUET_PATH = f"gs://{ML_BUCKET_TEMP}/algo_training_{ENV_SHORT_NAME}/{RUN_ID}/raw_input/data-*.parquet"
    METADATA_COLUMNS = ["item_id", "gtl_id", "artist_id"]

    DISPLAY_SUMMARY = False
    N_SAMPLES = 100
    RELEVANCE_THRESHOLDS = [0.3, 0.4, 0.5, 0.6, 0.7]
    GROUND_TRUTH_SCORE = "full_score"

    # ==================================================================================
    # Step 1: Load and index embedding table
    logger.info("=" * 50)
    logger.info("STEP 1: Loading/Opening LanceDB Embedding Table")
    logger.info("=" * 50)

    embedding_table = load_and_index_embeddings(
        parquet_path=PARQUET_PATH, table_name=TABLE_NAME, rebuild=False
    )

    # ==================================================================================
    # Step 2: Sample query nodes (LAZY - doesn't load full table)
    logger.info("=" * 50)
    logger.info("STEP 2: Sampling Query Nodes (Lazy)")
    logger.info("=" * 50)

    query_node_ids = sample_test_items_lazy(embedding_table, n_samples=N_SAMPLES)

    # ==================================================================================
    # Step 3: Generate predictions (LAZY)
    logger.info("=" * 50)
    logger.info("STEP 3: Generating Predictions (Lazy)")
    logger.info("=" * 50)

    df_results = generate_predictions_lazy(
        query_node_ids, table=embedding_table, n_retrieved=N_RETRIEVED
    )

    # ==================================================================================
    # Step 4: Join retrieval results with metadata
    logger.info("=" * 50)
    logger.info("STEP 4: Joining Retrieval Results with Metadata")
    logger.info("=" * 50)

    unique_node_ids = (
        pd.concat([df_results["query_node_id"], df_results["retrieved_node_id"]])
        .unique()
        .tolist()
    )

    metadata_table = load_metadata_table(
        parquet_path=METADATA_PARQUET_PATH,
        filter_field="item_id",
        filter_values=unique_node_ids,
        columns=METADATA_COLUMNS,
    )

    df_results = join_retrieval_with_metadata(
        retrieval_results=df_results,
        metadata_df=metadata_table,
        right_on="item_id",
    )

    # # ================================================================================
    # # Step 5: Compute ALL scores in one merged lazy pass (GTL + Artist + Full)
    logger.info("=" * 50)
    logger.info("STEP 5: Computing ALL Scores (Single Merged Pass)")
    logger.info("=" * 50)

    df_results = compute_all_scores_lazy(
        augmented_results=df_results,
        table=embedding_table,
        query_node_ids=query_node_ids,
        artist_col_query="query_artist_id",
        artist_col_retrieved="retrieved_artist_id",
        force_artist_weight=True,
    )

    # # ================================================================================
    # # Step 6: Compute Recommendation Metrics
    logger.info("=" * 50)
    logger.info("STEP 6: Computing Recommendation Metrics")
    logger.info("=" * 50)
    metrics, df_results = compute_retrieval_metrics(
        retrieval_results=df_results,
        k_values=K_VALUES_TO_EVAL,
        score_cols=GROUND_TRUTH_SCORE,
        relevancy_thresholds=RELEVANCE_THRESHOLDS,
    )

    logger.info("=" * 50)
    logger.info("Evaluation Complete!")
    logger.info("=" * 50)

    # logger.info("=" * 50)
    # logger.info("SUMMARY")
    # logger.info("=" * 50)
    # # Access metrics
    # logger.info("=" * 50)
    # logger.info(f"RECALL:\n{metrics['recall']['full_score']}")
    # logger.info("=" * 50)
    # logger.info(f"NDCG:\n{metrics['ndcg']['full_score']}")
    # logger.info("=" * 50)
    # logger.info(f"PRECISION:\n{metrics['precision']['full_score']}")
    # logger.info("=" * 50)

    # # Save results
    # output_path = RESULTS_DIR / f"retrieval_results_{RUN_ID}.parquet"
    # retrieval_results.to_parquet(output_path, index=False)
    # logger.info(f"Saved results to: {output_path}")
