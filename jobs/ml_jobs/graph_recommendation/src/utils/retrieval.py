import glob

import gcsfs
import lancedb
import numpy as np
import pandas as pd
from lancedb.pydantic import LanceModel, Vector
from loguru import logger
from tqdm import tqdm

from src.constants import DATA_DIR
from src.embedding_builder import EMBEDDING_COLUMN_NAME, EMBEDDING_DIM
from src.utils.commons import BUCKET_PREFIX, is_bucket_path

# Import your existing GTL scoring functions
from src.utils.metadata_metrics import (
    get_artist_score,
    get_gtl_retrieval_score,
    get_gtl_walk_score,
)

# Lance DB configuration

LANCEDB_BATCH_SIZE = 5000
NUM_PARTITIONS = 128
NUM_SUB_VECTORS = 16
INDEX_TYPE = "IVF_PQ"
EMBEDDING_METRIC = "cosine"
LANCEDB_PATH = DATA_DIR / "metadata/vector"


def create_book_embedding_model():
    class BookEmbeddingModel(LanceModel):
        node_ids: str
        gtl_id: str

    BookEmbeddingModel.__annotations__[EMBEDDING_COLUMN_NAME] = Vector(EMBEDDING_DIM)
    return BookEmbeddingModel


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
    db = lancedb.connect(LANCEDB_PATH)

    # Check if table exists
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

    non_null_columns = ["node_ids", "gtl_id", EMBEDDING_COLUMN_NAME]
    null_rows = df[df[non_null_columns].isnull().any(axis=1)]
    if not null_rows.empty:
        logger.error(
            f"Found {len(null_rows)} rows with null values in {non_null_columns}"
        )
        logger.info(f"Null rows sample:\n{null_rows.head()}")
        raise ValueError("DataFrame contains null values in critical columns.")

    # Create batches generator
    def make_batches(df: pd.DataFrame, batch_size: int):
        for i in range(0, len(df), batch_size):
            yield df[i : i + batch_size]

    # Drop existing table if rebuild
    if table_exists and rebuild:
        logger.info(f"Dropping existing table '{table_name}'")
        db.drop_table(table_name)

    # Create BookEmbeddingModel instance
    BookEmbeddingModel = create_book_embedding_model()

    # Create new table
    logger.info(f"Creating LanceDB table '{table_name}'")
    table = db.create_table(
        table_name,
        make_batches(df, LANCEDB_BATCH_SIZE),
        schema=BookEmbeddingModel,
    )
    logger.info(f"Table created with {len(table)} items")

    # Create vector index
    logger.info("Creating vector index...")
    table.create_index(
        vector_column_name=EMBEDDING_COLUMN_NAME,
        index_type=INDEX_TYPE,
        num_partitions=NUM_PARTITIONS,
        num_sub_vectors=NUM_SUB_VECTORS,
        metric=EMBEDDING_METRIC,
    )
    logger.info("Vector index created successfully")

    return table


def _get_matching_file_paths(parquet_path: str) -> list[str]:
    """Get list of matching file paths from GCS or local filesystem."""
    is_glob = "*" in parquet_path or "?" in parquet_path

    if is_bucket_path(parquet_path):
        fs = gcsfs.GCSFileSystem()
        if is_glob:
            matching_files = fs.glob(parquet_path)
            matching_files = [
                f"{BUCKET_PREFIX}{f}" if not is_bucket_path(f) else f
                for f in matching_files
            ]
        else:
            matching_files = [parquet_path]
    else:
        matching_files = glob.glob(parquet_path) if is_glob else [parquet_path]

    return matching_files


def _load_single_parquet_file(
    file_path: str, columns: list[str] | None = None
) -> pd.DataFrame:
    """Load a single parquet file from GCS or local filesystem."""
    import pandas as pd

    if is_bucket_path(file_path):
        fs = gcsfs.GCSFileSystem()
        with fs.open(file_path, "rb") as f:
            df_chunk = pd.read_parquet(f, columns=columns)
    else:
        df_chunk = pd.read_parquet(file_path, columns=columns)

    return df_chunk


def _filter_dataframe_by_field_values(
    df_chunk: pd.DataFrame, filter_field: str | None, filter_set: set[str] | None
) -> pd.DataFrame:
    """Apply filter to dataframe if filter_field and filter_set are specified."""
    if filter_field and filter_set:
        if filter_field in df_chunk.columns:
            df_chunk = df_chunk[df_chunk[filter_field].isin(filter_set)]
        else:
            logger.warning(f"Filter field '{filter_field}' not found in file")

    return df_chunk


def _load_and_filter_all_parquet_files(
    matching_files: list[str],
    columns: list[str] | None,
    filter_field: str | None,
    filter_set: set[str] | None,
) -> list[pd.DataFrame]:
    """Load all parquet files with filtering and return list of non-empty dataframes."""
    dfs = []

    for file_path in tqdm(matching_files, desc="Loading metadata"):
        df_chunk = _load_single_parquet_file(file_path, columns)
        df_chunk = _filter_dataframe_by_field_values(df_chunk, filter_field, filter_set)

        if len(df_chunk) > 0:
            dfs.append(df_chunk)

    return dfs


def _remove_duplicate_rows_by_field(
    df: pd.DataFrame, filter_field: str | None
) -> pd.DataFrame:
    """Remove duplicate rows based on filter_field if it exists."""
    if filter_field and filter_field in df.columns:
        df = df.drop_duplicates(subset=[filter_field], keep="first")

    return df


def load_metadata_table(
    parquet_path: str,
    filter_field: str | None = None,
    filter_values: list[str] | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Load metadata from parquet file(s) with optional filtering."""
    import pandas as pd

    logger.info(f"Loading metadata from: {parquet_path}")

    # Get file list
    matching_files = _get_matching_file_paths(parquet_path)
    logger.info(f"Found {len(matching_files)} file(s)")

    # Load and filter files
    filter_set = set(filter_values) if filter_values else None
    dfs = _load_and_filter_all_parquet_files(
        matching_files=matching_files,
        columns=columns,
        filter_field=filter_field,
        filter_set=filter_set,
    )

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # Remove duplicates if filter field exists
    df = _remove_duplicate_rows_by_field(df, filter_field)

    logger.info(f"Loaded {len(df)} rows")
    return df


def join_retrieval_with_metadata(
    retrieval_results: pd.DataFrame,
    metadata_df: pd.DataFrame,
    right_on: str = "node_ids",
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
    metadata_cols = [
        col
        for col in metadata_indexed.columns
        if col not in ["query_node_id", "retrieved_node_id"]
    ]

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
    return augmented_df


def _load_gtl_ids_into_lookup(
    unique_node_ids: np.ndarray, table: lancedb.table.Table
) -> dict[str, str]:
    """Load GTL IDs for unique node IDs from table into lookup dictionary."""
    gtl_lookup = {}
    batch_size = 5000

    for i in tqdm(range(0, len(unique_node_ids), batch_size), desc="Loading GTL IDs"):
        batch_ids = unique_node_ids[i : i + batch_size]
        # Create filter for batch
        id_list = "','".join(batch_ids)
        where_clause = f"node_ids IN ('{id_list}')"
        # Query only these items
        batch_results = (
            table.search().where(where_clause).limit(len(batch_ids)).to_pandas()
        )
        # Add to lookup
        for _, row in batch_results.iterrows():
            gtl_lookup[row["node_ids"]] = row["gtl_id"]

    logger.info(f"Loaded {len(gtl_lookup)} GTL mappings")
    return gtl_lookup


def _initialize_result_score_columns(augmented_results: pd.DataFrame) -> None:
    """Initialize all score columns in the dataframe with default values."""
    augmented_results["gtl_retrieval_score"] = 0.0
    augmented_results["gtl_walk_score"] = 0.0
    augmented_results["artist_score"] = 0.0
    augmented_results["full_score"] = 0.0
    augmented_results["has_artist"] = False


def _process_all_results_for_query(
    augmented_results: pd.DataFrame,
    query_id: str,
    gtl_lookup: dict[str, str],
    artist_col_query: str,
    artist_col_retrieved: str,
    *,
    has_artist_cols: bool,
    force_artist_weight: bool,
) -> None:
    """Process all results for a single query with chunking."""
    # Get all results for this query
    mask = augmented_results["query_node_id"] == query_id
    query_results_indices = augmented_results[mask].index

    # Lookup query GTL once (reused for all this query's results)
    query_gtl = gtl_lookup.get(query_id)

    # Process this query's results in chunks
    chunk_size = 5000
    for start in range(0, len(query_results_indices), chunk_size):
        chunk_indices = query_results_indices[start : start + chunk_size]
        chunk = augmented_results.loc[chunk_indices]

        # Compute scores for this chunk
        gtl_retrieval_scores, gtl_walk_scores, artist_scores, has_artist = (
            _compute_chunk_scores(
                chunk=chunk,
                query_gtl=query_gtl,
                gtl_lookup=gtl_lookup,
                has_artist_cols=has_artist_cols,
                artist_col_query=artist_col_query,
                artist_col_retrieved=artist_col_retrieved,
            )
        )

        # Compute full scores
        full_scores = _calculate_weighted_final_score(
            gtl_retrieval_scores=gtl_retrieval_scores,
            artist_scores=artist_scores,
            has_artist=has_artist,
            force_artist_weight=force_artist_weight,
        )

        # Update dataframe
        augmented_results.loc[chunk_indices, "gtl_retrieval_score"] = (
            gtl_retrieval_scores
        )
        augmented_results.loc[chunk_indices, "gtl_walk_score"] = gtl_walk_scores
        augmented_results.loc[chunk_indices, "artist_score"] = artist_scores
        augmented_results.loc[chunk_indices, "full_score"] = full_scores
        augmented_results.loc[chunk_indices, "has_artist"] = has_artist


def _compute_chunk_scores(
    chunk: pd.DataFrame,
    query_gtl: str | None,
    gtl_lookup: dict[str, str],
    artist_col_query: str,
    artist_col_retrieved: str,
    *,
    has_artist_cols: bool,
) -> tuple[list[float], list[float], np.ndarray, np.ndarray]:
    """Compute GTL and artist scores for a chunk of results."""
    # Get retrieved GTLs
    retrieved_gtls = chunk["retrieved_node_id"].map(gtl_lookup)

    # Compute GTL scores
    gtl_retrieval_scores = [
        get_gtl_retrieval_score(query_gtl, r_gtl) for r_gtl in retrieved_gtls
    ]
    gtl_walk_scores = [get_gtl_walk_score(query_gtl, r_gtl) for r_gtl in retrieved_gtls]

    # Compute artist scores
    if has_artist_cols:
        artist_scores = chunk.apply(
            lambda row: get_artist_score(
                row[artist_col_query], row[artist_col_retrieved]
            ),
            axis=1,
        ).values
        has_artist = chunk[artist_col_query].notna().values
    else:
        artist_scores = np.zeros(len(chunk))
        has_artist = np.zeros(len(chunk), dtype=bool)

    return gtl_retrieval_scores, gtl_walk_scores, artist_scores, has_artist


def _calculate_weighted_final_score(
    gtl_retrieval_scores: list[float],
    artist_scores: np.ndarray,
    has_artist: np.ndarray,
    *,
    force_artist_weight: bool,
) -> np.ndarray:
    """Calculate final combined score with artist weighting."""
    if force_artist_weight:
        full_scores = (np.array(gtl_retrieval_scores) + artist_scores) / 2.0
    else:
        full_scores = np.where(
            has_artist,
            (np.array(gtl_retrieval_scores) + artist_scores) / 2.0,
            gtl_retrieval_scores,
        )
    return full_scores


def _log_score_statistics(
    augmented_results: pd.DataFrame,
    *,
    has_artist_cols: bool,
) -> None:
    """Log comprehensive statistics for all computed scores."""
    logger.info("\n📊 GTL Retrieval Scores:")
    logger.info(f"  Mean: {augmented_results['gtl_retrieval_score'].mean():.3f}")
    logger.info(f"  Median: {augmented_results['gtl_retrieval_score'].median():.3f}")
    logger.info(f"  Min: {augmented_results['gtl_retrieval_score'].min():.3f}")
    logger.info(f"  Max: {augmented_results['gtl_retrieval_score'].max():.3f}")

    logger.info("\n🚶 GTL Walk Scores:")
    logger.info(f"  Mean: {augmented_results['gtl_walk_score'].mean():.3f}")
    logger.info(f"  Median: {augmented_results['gtl_walk_score'].median():.3f}")
    logger.info(f"  Min: {augmented_results['gtl_walk_score'].min():.3f}")
    logger.info(f"  Max: {augmented_results['gtl_walk_score'].max():.3f}")

    if has_artist_cols:
        n_with_artist = augmented_results["has_artist"].sum()
        total_rows = len(augmented_results)
        logger.info("\n🎵 Artist Scores:")
        logger.info(
            f"  Queries with artist: {n_with_artist}/{total_rows} "
            f"({100 * n_with_artist / total_rows:.1f}%)"
        )
        logger.info(f"  Mean (all): {augmented_results['artist_score'].mean():.3f}")
        logger.info(
            f"  Matches: {int(augmented_results['artist_score'].sum())}/{n_with_artist}"
        )
        if n_with_artist > 0:
            artist_mean = augmented_results[augmented_results["has_artist"]][
                "artist_score"
            ].mean()
            logger.info(f"  Mean (with artist): {artist_mean:.3f}")
    else:
        logger.info("\n🎵 Artist Scores: N/A (columns not found)")

    logger.info("\n✨ Full Scores (gtl_retrieval + artist):")
    logger.info(f"  Mean (all): {augmented_results['full_score'].mean():.3f}")
    logger.info(f"  Median: {augmented_results['full_score'].median():.3f}")

    if has_artist_cols and augmented_results["has_artist"].sum() > 0:
        with_artist_mean = augmented_results[augmented_results["has_artist"]][
            "full_score"
        ].mean()
        without_artist_mean = augmented_results[~augmented_results["has_artist"]][
            "full_score"
        ].mean()
        logger.info(f"  Mean (with artist): {with_artist_mean:.3f}")
        logger.info(f"  Mean (no artist): {without_artist_mean:.3f}")

    logger.info("=" * 80)


def compute_all_scores_lazy(
    augmented_results: pd.DataFrame,
    table: lancedb.table.Table,
    query_node_ids: list[str] | None = None,
    artist_col_query: str = "query_artist_id",
    artist_col_retrieved: str = "retrieved_artist_id",
    *,
    force_artist_weight: bool = False,
) -> pd.DataFrame:
    """
    Compute GTL, artist, and combined scores for retrieval results.

    Performs efficient query-aware scoring by loading GTL IDs lazily (only for items
    in results), processing results grouped by query for better cache locality, and
    reusing each query's GTL for all its retrieved items.

    Computed Scores:
        - GTL retrieval scores: Asymmetric depth-normalized scoring
        - GTL walk scores: Symmetric distance-based scoring
        - Artist matching scores: Binary match between query and retrieved artists
        - Full combined score: Weighted combination of GTL and artist scores

    Scoring Behavior:
        - Missing GTL mappings result in score of 0.0
        - Artist scoring requires both query and retrieved items to have non-null
          artist IDs for a match (score of 1.0)
        - When force_artist_weight=False, items without artist metadata use
          gtl_retrieval_score as the full_score
        - When force_artist_weight=True, all items use the averaged score formula

    Args:
        augmented_results: DataFrame with retrieval results and metadata.
            Required columns:
                - query_node_id: Query item identifier
                - retrieved_node_id: Retrieved item identifier
            Optional columns (if artist scoring enabled):
                - {artist_col_query}: Artist ID for query item
                - {artist_col_retrieved}: Artist ID for retrieved item
        table: LanceDB table containing embeddings and GTL mappings.
            Must have columns: node_ids, gtl_id
        query_node_ids: List of query IDs to process. If None, processes all queries
            in augmented_results.
        artist_col_query: Column name for query item's artist ID.
            Default: "query_artist_id"
        artist_col_retrieved: Column name for retrieved item's artist ID.
            Default: "retrieved_artist_id"
        force_artist_weight: Scoring strategy for items without artist metadata.
            - If True: Always use (gtl_score + artist_score) / 2, even when
              artist is None (artist_score = 0)
            - If False: Use gtl_score only when artist is None
            Default: False

    Returns:
        DataFrame: Input dataframe with additional score columns:
            - gtl_retrieval_score (float): Asymmetric depth-normalized GTL score [0, 1]
            - gtl_walk_score (float): Symmetric distance-based GTL score [0, 1]
            - artist_score (float): Binary artist match score (0 or 1)
            - full_score (float): Combined final score [0, 1]
            - has_artist (bool): Whether query item has artist metadata

    Raises:
        KeyError: If required columns are missing from augmented_results

    Implementation Details:
        - GTL Lookup: Lazy loading - only fetches GTL IDs for items present in
          results, not the entire catalog. Uses batch queries of 5000 items.
        - Query Processing: Processes results grouped by query. Each query's GTL
          is looked up once and reused for all its retrieved items (better cache
          locality). Results within each query are processed in chunks of 5000.
        - Progress Tracking: Shows progress bars for GTL loading and per-query scoring
        - Logging: Comprehensive statistics including score distributions and
          artist matching rates

    """
    logger.info("=" * 80)
    logger.info("MERGED SCORING: Computing GTL + Artist + Full Scores (Per-Query)")
    logger.info("=" * 80)

    # Get unique node IDs we need for GTL lookup
    unique_node_ids = pd.concat(
        [augmented_results["query_node_id"], augmented_results["retrieved_node_id"]]
    ).unique()
    logger.info(f"Need GTL IDs for {len(unique_node_ids)} unique items")

    # PHASE 1: Build GTL lookup (lazy - only needed items)
    gtl_lookup = _load_gtl_ids_into_lookup(unique_node_ids, table)

    # PHASE 2: Compute all scores per-query with sub-batching
    logger.info("Computing all scores per-query...")

    # Initialize score columns
    _initialize_result_score_columns(augmented_results)

    # Check if artist columns exist
    has_artist_cols = (
        artist_col_query in augmented_results.columns
        and artist_col_retrieved in augmented_results.columns
    )

    # Process per query
    for query_id in tqdm(query_node_ids, desc="Scoring queries"):
        _process_all_results_for_query(
            augmented_results=augmented_results,
            query_id=query_id,
            gtl_lookup=gtl_lookup,
            has_artist_cols=has_artist_cols,
            artist_col_query=artist_col_query,
            artist_col_retrieved=artist_col_retrieved,
            force_artist_weight=force_artist_weight,
        )

    # Log statistics
    _log_score_statistics(augmented_results, has_artist_cols=has_artist_cols)

    return augmented_results


def sample_test_items_lazy(
    table: lancedb.table.Table,
    n_samples: int,
    random_state: int = 42,
) -> list[str]:
    """
    Sample node IDs from LanceDB table without loading full table into memory.

    TODO: Sample from dataframe instead of lancedb

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
    results = table.search().where(f"node_ids = '{node_id}'").limit(1).to_pandas()

    if len(results) == 0:
        return None

    return results.iloc[0][EMBEDDING_COLUMN_NAME]


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

    for query_item in tqdm(query_node_ids, desc="Retrieving"):
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
            retrieved_id = row["node_ids"]

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
