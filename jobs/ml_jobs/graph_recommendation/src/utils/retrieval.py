from __future__ import annotations

from typing import TYPE_CHECKING

import lancedb
import numpy as np
import pandas as pd
from loguru import logger

from src.constants import (
    DATA_DIR,
    EMBEDDING_COLUMN,
    FULL_SCORE_COLUMN,
    GTL_ID_COLUMN,
    LANCEDB_NODE_ID_COLUMN,
)

# Import your existing GTL scoring functions

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

# Lance DB configuration

LANCEDB_BATCH_SIZE = 5000
NUM_PARTITIONS = 128
NUM_SUB_VECTORS = 16
INDEX_TYPE = "IVF_PQ"
EMBEDDING_METRIC = "cosine"
LANCEDB_PATH = f"{DATA_DIR}/metadata/vector"
LANCEDB_TABLE_NAME = "embedding_table"
NON_NULL_COLUMNS = [LANCEDB_NODE_ID_COLUMN, EMBEDDING_COLUMN]


def chunk_dataframe(
    df: pd.DataFrame, batch_size: int
) -> Generator[pd.DataFrame, None, None]:
    """Yield successive batches from DataFrame."""
    for start_index in range(0, len(df), batch_size):
        yield df[start_index : start_index + batch_size]


def load_and_index_embeddings(
    parquet_path: str,
    table_name: str,
    *,
    rebuild: bool = False,
) -> lancedb.table.Table:
    """
    Load embeddings from Parquet and create/open LanceDB table.

    Args:
        parquet_path: Path to parquet file (local or GCS)
        table_name: Name of LanceDB table
        rebuild: If True, drop existing table and rebuild

    Returns:
        LanceDB table
    """
    # Check if table exists and not rebuilding
    db = lancedb.connect(LANCEDB_PATH)
    existing_tables = db.table_names()
    table_exists = table_name in existing_tables

    if table_exists and not rebuild:
        logger.info(f"Opening existing LanceDB table '{table_name}'")
        table = db.open_table(table_name)
        logger.info(f"Table has {len(table)} items")
        return table

    # Load data from parquet
    logger.info(f"Loading embeddings from: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} items from parquet")

    null_rows = df[df[NON_NULL_COLUMNS].isnull().any(axis=1)]
    if not null_rows.empty:
        logger.error(
            f"Found {len(null_rows)} rows with null values in {NON_NULL_COLUMNS}"
        )
        logger.info(f"Null rows sample:\n{null_rows.head()}")
        raise ValueError("DataFrame contains null values in critical columns.")

    # Drop existing table if rebuild
    if table_exists and rebuild:
        logger.info(f"Dropping existing table '{table_name}'")
        db.drop_table(table_name)

    # Create new table
    logger.info(f"Creating LanceDB table '{table_name}'")
    table = db.create_table(
        table_name,
        chunk_dataframe(df, LANCEDB_BATCH_SIZE),
    )
    logger.info(f"Table created with {len(table)} items")

    # Create vector index
    logger.info("Creating vector index...")
    table.create_index(
        vector_column_name=EMBEDDING_COLUMN,
        index_type=INDEX_TYPE,
        num_partitions=NUM_PARTITIONS,
        num_sub_vectors=NUM_SUB_VECTORS,
        metric=EMBEDDING_METRIC,
    )
    logger.info("Vector index created successfully")

    return table


def join_retrieval_with_metadata(
    retrieval_results: pd.DataFrame,
    metadata_df: pd.DataFrame,
    right_on: str,
) -> pd.DataFrame:
    """
    Join retrieval results with metadata for both query and retrieved items.

    This function performs two left joins:
    1. Joins metadata for query items
                on retrieval_results.query_node_id = metadata[right_on]
    2. Joins metadata for retrieved items
                on retrieval_results.retrieved_node_id = metadata[right_on]

    Metadata columns are prefixed with 'query_' and 'retrieved_' to distinguish them.

    Args:
        retrieval_results: DataFrame with retrieval results.
            Required columns:
                - query_node_id: The query item identifier
                - retrieved_node_id: The retrieved item identifier
            Note: These column names are hardcoded and cannot be changed.
        metadata_df: DataFrame with metadata to join.
            Must contain the column specified in right_on parameter.
        right_on: Column name in metadata_df to join on. This column should contain
            item identifiers that match both query_node_id and retrieved_node_id values.
            Default: "node_ids"

    Returns:
        Augmented DataFrame with original columns plus:
            - query_{col}: Metadata columns for query items
                            (prefixed with 'query_')
            - retrieved_{col}: Metadata columns for retrieved items
                                (prefixed with 'retrieved_')

    Raises:
        KeyError: If retrieval_results is missing required columns
                    (query_node_id, retrieved_node_id)
        KeyError: If metadata_df is missing the column specified in right_on
    """
    logger.info("Joining retrieval results with metadata")

    metadata_indexed = metadata_df.set_index(right_on)
    metadata_cols = metadata_indexed.columns

    # Join for query & retrieved items
    augmented_df = (
        retrieval_results.merge(
            metadata_indexed,
            left_on="query_node_id",
            right_index=True,
            how="left",
            suffixes=("", "_query"),
        )
        .rename(columns={col: f"query_{col}" for col in metadata_cols})
        .merge(
            metadata_indexed,
            left_on="retrieved_node_id",
            right_index=True,
            how="left",
            suffixes=("", "_retrieved"),
        )
        .rename(columns={col: f"retrieved_{col}" for col in metadata_cols})
    )

    logger.info(f"Augmented results shape: {augmented_df.shape}")
    return augmented_df.where(lambda df: pd.notna(df), None)


def _log_score_statistics(
    augmented_results: pd.DataFrame,
) -> None:
    logger.info("=" * 80)
    for column in augmented_results.columns:
        if column.endswith("_score"):
            logger.info(f"\n📊 {column.replace('_score', '').capitalize()} Scores:")
            logger.info(f"  Mean: {augmented_results[column].mean():.3f}")
            logger.info(f"  Median: {augmented_results[column].median():.3f}")
            logger.info(f"  Min: {augmented_results[column].min():.3f}")
            logger.info(f"  Max: {augmented_results[column].max():.3f}")
            logger.info("=" * 80)


def compute_all_scores(
    retrieved_results_with_metadata_df: pd.DataFrame,
    metadata_columns: list[str],
    categorical_metadata_columns: list[str],
    hierarchical_metadata_scoring_functions: dict[str, Callable[[str, str], float]],
) -> pd.DataFrame:
    # 1. Compute categorical scores
    df_with_categorical_score = retrieved_results_with_metadata_df.assign(
        **{
            f"{column}_score": (
                retrieved_results_with_metadata_df[f"query_{column}"]
                == retrieved_results_with_metadata_df[f"retrieved_{column}"]
            )
            .astype(int)
            .where(retrieved_results_with_metadata_df[f"query_{column}"].notna(), None)
            for column in categorical_metadata_columns
        }
    )

    # 2. Compute hierarchical scores
    df_with_hierarchical_score = df_with_categorical_score.assign(
        **{
            f"{column}_score": df_with_categorical_score.apply(
                lambda row, col=column, eval_fn=hierarchical_evaluation_function: (
                    eval_fn(row[f"query_{col}"], row[f"retrieved_{col}"])
                ),
                axis=1,
            )
            for column, hierarchical_evaluation_function in hierarchical_metadata_scoring_functions.items()  # noqa: E501
        }
    )

    # 3. Compute full combined scores
    df_with_full_scores = df_with_hierarchical_score.assign(
        **{
            FULL_SCORE_COLUMN: df_with_hierarchical_score[
                [f"{column}_score" for column in metadata_columns]
            ].mean(axis=1)
        }
    )

    # 4. Log statistics
    _log_score_statistics(df_with_full_scores)

    return df_with_full_scores


def sample_test_items_lazy(
    table: lancedb.table.Table,
    n_samples: int,
    random_state: int = 42,
) -> list[str]:
    """
    Sample node IDs from LanceDB table without loading full table into memory.

    TODO: Sample from dataframe instead of lancedb
            & filter out ill defined data (gtl 0000000, etc)

    Args:
        table: LanceDB table containing items to sample from.
            Must have a 'node_ids' column.
        n_samples: Number of items to sample. If greater than table size,
            samples all items. Default: N_SAMPLES
        random_state: Random seed for reproducible sampling. Default: 42

    Returns:
        List of sampled node IDs (strings) in the order they were sampled.
        Length will be min(n_samples, total_table_size).
    """

    logger.info("Sampling test items from LanceDB")

    # Get total count (doesn't load data)
    total_count = len(table)
    logger.info(f"Total items in table: {total_count}")

    # Sample random row indices
    np.random.seed(random_state)
    sample_size = min(n_samples, total_count)
    rng = np.random.default_rng(random_state)
    sampled_indices = rng.choice(total_count, size=sample_size, replace=False)

    # Read sampled rows in one call
    scanner = table.to_lance()
    sampled_df = scanner.take(sampled_indices).to_pandas()
    query_node_ids = sampled_df["node_ids"].tolist()

    logger.info(f"Sampled {len(query_node_ids)} items")
    return query_node_ids


def get_embedding_for_item_lazy(
    table: lancedb.table.Table,
    node_id: str,
) -> list[float] | None:
    """
    Get embedding for a single item (no full table load).
    """
    # Search with filter - only loads matching rows
    results = (
        table.search()
        .where(f"{LANCEDB_NODE_ID_COLUMN} = '{node_id}'")
        .limit(1)
        .to_pandas()
    )

    if len(results) == 0:
        return None

    return results.iloc[0][EMBEDDING_COLUMN]


def generate_predictions_lazy(
    query_node_ids: list[str],
    table: lancedb.table.Table,
    n_retrieved: int,
) -> pd.DataFrame:
    """
    Retrieve top-n_retrieved similar items for each query node.
    Predicted score  -> embedding similarity

    Args:
        query_node_ids: Query items
        table: LanceDB table
        n_retrieved: Number of results per query
    """
    logger.info(
        f"Retrieving top-{n_retrieved} similar items for {len(query_node_ids)} queries"
    )

    results = []

    for query_item in query_node_ids:
        # Get query embedding
        query_embedding = get_embedding_for_item_lazy(table, query_item)

        if query_embedding is None:
            logger.warning(f"No embedding found for: {query_item}")
            continue

        # Search LanceDB (only loads n_retrieved+1 results)
        search_results = (
            table.search(query_embedding).limit(n_retrieved + 1).to_pandas()
        )

        # Process results
        rank = 0
        for _, row in search_results.iterrows():
            retrieved_id = row[LANCEDB_NODE_ID_COLUMN]

            # Skip self-match
            if str(retrieved_id) == str(query_item):
                continue

            rank += 1
            if rank > n_retrieved:
                break

            # Convert distance to similarity
            distance = row.get("_distance", 0)
            similarity_score = (
                1.0 - distance if EMBEDDING_METRIC == "cosine" else distance
            )

            results.append(
                {
                    "query_node_id": str(query_item),
                    "retrieved_node_id": str(retrieved_id),
                    "similarity_score": float(similarity_score),
                    "rank": rank,
                }
            )

    df_results = pd.DataFrame(results)
    logger.info(f"Retrieved {len(df_results)} (query, item) pairs")
    return df_results
