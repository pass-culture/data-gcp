import gcsfs
import lancedb
import numpy as np
import pandas as pd
from lancedb.pydantic import LanceModel, Vector
from loguru import logger
from tqdm import tqdm

from src.constants import ENV_SHORT_NAME, LANCEDB_PATH, ML_BUCKET_TEMP

# Import your existing GTL scoring functions
from src.utils.metadata_metrics import (
    get_artist_score,
    get_gtl_retrieval_score,
    get_gtl_walk_score,
)
from src.utils.recommendation_metrics import compute_retrieval_metrics

# Configuration

ITEM_BATCH_SIZE = 50
LANCEDB_BATCH_SIZE = 5000
NUM_PARTITIONS = 128
NUM_SUB_VECTORS = 16
RETRIEVAL_FILTERS = []
INDEX_TYPE = "IVF_PQ"
EMBEDDING_METRIC = "cosine"
N_SAMPLES = 10000


class BookModel(LanceModel):
    """Schema for items in LanceDB."""

    embeddings: Vector(32)
    node_ids: str
    gtl_id: str


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

    if parquet_path.startswith("gs://"):
        fs = gcsfs.GCSFileSystem()
        with fs.open(parquet_path, "rb") as f:
            df = pd.read_parquet(f)
    else:
        df = pd.read_parquet(parquet_path)

    logger.info(f"Loaded {len(df)} items from parquet")

    # Drop nulls
    df = df.dropna(subset=["node_ids", "gtl_id", "embeddings"])
    logger.info(f"After dropping nulls: {len(df)} items")

    # Create batches generator
    def make_batches(df: pd.DataFrame, batch_size: int):
        for i in range(0, len(df), batch_size):
            yield df[i : i + batch_size]

    # Drop existing table if rebuild
    if table_exists and rebuild:
        logger.info(f"Dropping existing table '{table_name}'")
        db.drop_table(table_name)

    # Create new table
    logger.info(f"Creating LanceDB table '{table_name}'")
    table = db.create_table(
        table_name,
        make_batches(df, LANCEDB_BATCH_SIZE),
        schema=BookModel,
    )
    logger.info(f"Table created with {len(table)} items")

    # Create vector index
    logger.info("Creating vector index...")
    table.create_index(
        vector_column_name="embeddings",
        index_type=INDEX_TYPE,
        num_partitions=NUM_PARTITIONS,
        num_sub_vectors=NUM_SUB_VECTORS,
        metric=EMBEDDING_METRIC,
    )
    logger.info("Vector index created successfully")

    # Create scalar indices if needed
    for feature in RETRIEVAL_FILTERS:
        logger.info(f"Creating scalar index on: {feature}")
        table.create_scalar_index(feature, index_type="BITMAP")

    return table


def load_metadata_table(
    parquet_path: str,
    filter_field: str | None = None,
    filter_values: list[str] | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Load metadata from parquet file(s) with optional filtering.

    Args:
        parquet_path: Path to parquet file(s), supports glob patterns
        filter_field: Column name to filter on (optional)
        filter_values: List of values to keep (optional, requires filter_field)
        columns: Optional list of columns to load

    Returns:
        DataFrame with metadata
    """
    import glob

    logger.info(f"Loading metadata from: {parquet_path}")

    # Get file list
    is_glob = "*" in parquet_path or "?" in parquet_path

    if parquet_path.startswith("gs://"):
        fs = gcsfs.GCSFileSystem()
        if is_glob:
            matching_files = fs.glob(parquet_path)
            matching_files = [
                f"gs://{f}" if not f.startswith("gs://") else f for f in matching_files
            ]
        else:
            matching_files = [parquet_path]
    else:
        matching_files = glob.glob(parquet_path) if is_glob else [parquet_path]

    logger.info(f"Found {len(matching_files)} file(s)")

    # Load and optionally filter
    filter_set = set(filter_values) if filter_values else None
    dfs = []

    for file_path in tqdm(matching_files, desc="Loading metadata"):
        if file_path.startswith("gs://"):
            fs = gcsfs.GCSFileSystem()
            with fs.open(file_path, "rb") as f:
                df_chunk = pd.read_parquet(f, columns=columns)
        else:
            df_chunk = pd.read_parquet(file_path, columns=columns)

        # Apply filter if specified
        if filter_field and filter_set:
            if filter_field in df_chunk.columns:
                df_chunk = df_chunk[df_chunk[filter_field].isin(filter_set)]
            else:
                logger.warning(f"Filter field '{filter_field}' not found in file")

        if len(df_chunk) > 0:
            dfs.append(df_chunk)

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # Remove duplicates if filter field exists
    if filter_field and filter_field in df.columns:
        df = df.drop_duplicates(subset=[filter_field], keep="first")

    logger.info(f"Loaded {len(df)} rows")
    return df


def join_retrieval_with_metadata(
    retrieval_results: pd.DataFrame,
    metadata_df: pd.DataFrame,
    left_on: str = "node_ids",
    right_on: str = "node_ids",
) -> pd.DataFrame:
    """
    Join retrieval results with metadata for both query and retrieved items.

    Args:
        retrieval_results: DataFrame with query_node_id, retrieved_node_id
        metadata_df: DataFrame with metadata
        left_on: Column name in retrieval_results to join on
        right_on: Column name in metadata_df to join on

    Returns:
        Augmented DataFrame with metadata for both query and retrieved items
    """
    logger.info("Joining retrieval results with metadata")

    metadata_indexed = metadata_df.set_index(right_on)
    metadata_cols = [
        col
        for col in metadata_indexed.columns
        if col not in ["query_node_id", "retrieved_node_id"]
    ]

    # Join for query items
    augmented = retrieval_results.merge(
        metadata_indexed,
        left_on="query_node_id",
        right_index=True,
        how="left",
        suffixes=("", "_query"),
    )
    rename_dict = {col: f"query_{col}" for col in metadata_cols}
    augmented = augmented.rename(columns=rename_dict)

    # Join for retrieved items
    augmented = augmented.merge(
        metadata_indexed,
        left_on="retrieved_node_id",
        right_index=True,
        how="left",
        suffixes=("", "_retrieved"),
    )
    rename_dict = {col: f"retrieved_{col}" for col in metadata_cols}
    augmented = augmented.rename(columns=rename_dict)

    logger.info(f"Augmented results shape: {augmented.shape}")
    return augmented


def compute_all_scores_lazy(
    augmented_results: pd.DataFrame,
    table: lancedb.table.Table,
    artist_col_query: str = "query_artist_id",
    artist_col_retrieved: str = "retrieved_artist_id",
    batch_size: int = 1000,
    *,
    force_artist_weight: bool = False,
) -> pd.DataFrame:
    """
    Args:
        augmented_results: DataFrame with retrieval results and metadata
        table: LanceDB table for GTL lookups
        artist_col_query: Column name for query artist ID
        artist_col_retrieved: Column name for retrieved artist ID
        batch_size: Batch size for GTL lookups
        force_artist_weight: If True, always use (gtl + artist)/2 even when no artist.
                            If False, use gtl only when no artist.

    Returns:
        DataFrame with all scores computed: gtl_retrieval_score, gtl_walk_score,
        artist_score, full_score, has_artist
    """
    logger.info("=" * 80)
    logger.info("MERGED SCORING: Computing GTL + Artist + Full Scores (Single Pass)")
    logger.info("=" * 80)

    # Get unique node IDs we need for GTL lookup
    unique_node_ids = pd.concat(
        [augmented_results["query_node_id"], augmented_results["retrieved_node_id"]]
    ).unique()

    logger.info(f"Need GTL IDs for {len(unique_node_ids)} unique items")

    # =========================================================================
    # PHASE 1: Build GTL lookup (lazy - only needed items)
    # =========================================================================
    gtl_lookup = {}

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

    # =========================================================================
    # PHASE 2: Compute all scores in batches (vectorized where possible)
    # =========================================================================
    logger.info("Computing all scores in batches...")

    # Prepare result lists
    gtl_retrieval_scores = []
    gtl_walk_scores = []
    artist_scores = []
    full_scores = []
    has_artist_flags = []

    total_rows = len(augmented_results)

    # Check if artist columns exist
    has_artist_cols = (
        artist_col_query in augmented_results.columns
        and artist_col_retrieved in augmented_results.columns
    )

    # Process in batches for better performance
    for batch_start in tqdm(range(0, total_rows, batch_size), desc="Scoring batches"):
        batch_end = min(batch_start + batch_size, total_rows)
        batch = augmented_results.iloc[batch_start:batch_end]

        # --- GTL Scores ---
        query_gtls = batch["query_node_id"].map(gtl_lookup)
        retrieved_gtls = batch["retrieved_node_id"].map(gtl_lookup)

        batch_gtl_retrieval = np.zeros(len(batch))
        batch_gtl_walk = np.zeros(len(batch))

        # Compute scores for valid pairs
        for idx, (q_gtl, r_gtl) in enumerate(
            zip(query_gtls, retrieved_gtls, strict=True)
        ):
            batch_gtl_retrieval[idx] = get_gtl_retrieval_score(q_gtl, r_gtl)
            batch_gtl_walk[idx] = get_gtl_walk_score(q_gtl, r_gtl)

        # --- Artist Scores ---
        if has_artist_cols:
            batch_artist = batch.apply(
                lambda row: get_artist_score(
                    row[artist_col_query], row[artist_col_retrieved]
                ),
                axis=1,
            ).values

            # Check which queries have artist info
            batch_has_artist = batch[artist_col_query].notna().values
        else:
            batch_artist = np.zeros(len(batch))
            batch_has_artist = np.zeros(len(batch), dtype=bool)

        # --- Full Scores ---
        if force_artist_weight:
            # Always use average, even if no artist
            batch_full = (batch_gtl_retrieval + batch_artist) / 2.0
        else:
            # If query has artist: (gtl + artist) / 2, else: gtl only
            batch_full = np.where(
                batch_has_artist,
                (batch_gtl_retrieval + batch_artist) / 2.0,
                batch_gtl_retrieval,
            )

        # Append to result lists
        gtl_retrieval_scores.extend(batch_gtl_retrieval)
        gtl_walk_scores.extend(batch_gtl_walk)
        artist_scores.extend(batch_artist)
        full_scores.extend(batch_full)
        has_artist_flags.extend(batch_has_artist)

    # =========================================================================
    # PHASE 3: Add all scores to DataFrame
    # =========================================================================
    augmented_results["gtl_retrieval_score"] = gtl_retrieval_scores
    augmented_results["gtl_walk_score"] = gtl_walk_scores
    augmented_results["artist_score"] = artist_scores
    augmented_results["full_score"] = full_scores
    augmented_results["has_artist"] = has_artist_flags

    # =========================================================================
    # PHASE 4: Log comprehensive statistics
    # =========================================================================
    logger.info("=" * 80)
    logger.info("SCORING COMPLETE - Statistics:")
    logger.info("=" * 80)

    logger.info("\n GTL Retrieval Scores (Asymmetric):")
    logger.info(f"  Mean: {np.mean(gtl_retrieval_scores):.3f}")
    logger.info(f"  Median: {np.median(gtl_retrieval_scores):.3f}")
    logger.info(f"  Min: {np.min(gtl_retrieval_scores):.3f}")
    logger.info(f"  Max: {np.max(gtl_retrieval_scores):.3f}")

    logger.info("\n GTL Walk Scores (Symmetric):")
    logger.info(f"  Mean: {np.mean(gtl_walk_scores):.3f}")
    logger.info(f"  Median: {np.median(gtl_walk_scores):.3f}")
    logger.info(f"  Min: {np.min(gtl_walk_scores):.3f}")
    logger.info(f"  Max: {np.max(gtl_walk_scores):.3f}")

    if has_artist_cols:
        n_with_artist = sum(has_artist_flags)
        logger.info("\n Artist Scores:")
        logger.info(
            f"""  Queries with artist:
            {n_with_artist}/{total_rows} ({100 * n_with_artist / total_rows:.1f}%)
            """
        )
        logger.info(f"  Mean (all): {np.mean(artist_scores):.3f}")
        logger.info(f"  Matches: {int(sum(artist_scores))}/{n_with_artist}")
        if n_with_artist > 0:
            artist_scores_array = np.array(artist_scores)
            has_artist_array = np.array(has_artist_flags)
            logger.info(
                f"""  Mean (with artist):
                {artist_scores_array[has_artist_array].mean():.3f}"""
            )
    else:
        logger.info("\n Artist Scores: N/A (columns not found)")

    logger.info("\n Full Scores (gtl_retrieval + artist):")
    logger.info(f"  Mean (all): {np.mean(full_scores):.3f}")
    logger.info(f"  Median: {np.median(full_scores):.3f}")
    if has_artist_cols and sum(has_artist_flags) > 0:
        full_scores_array = np.array(full_scores)
        has_artist_array = np.array(has_artist_flags)
        logger.info(
            f"  Mean (with artist): {full_scores_array[has_artist_array].mean():.3f}"
        )
        logger.info(
            f"  Mean (no artist): {full_scores_array[~has_artist_array].mean():.3f}"
        )

    logger.info("=" * 80)

    return augmented_results


def sample_test_items_lazy(
    table: lancedb.table.Table,
    n_samples: int = N_SAMPLES,
    random_state: int = 42,
) -> list[str]:
    """
    Sample items without loading full table into memory.
    Uses random sampling on indices.
    """
    logger.info("Sampling test items from LanceDB (lazy)")

    # Get total count (doesn't load data)
    total_count = len(table)
    logger.info(f"Total items in table: {total_count}")

    # Sample random row indices
    np.random.seed(random_state)
    sample_size = min(n_samples, total_count)
    sampled_indices = sorted(
        np.random.choice(total_count, size=sample_size, replace=False)
    )

    # Use scanner to read only sampled rows (lazy!)
    scanner = table.to_lance()

    # Batch read sampled rows
    batch_size = 1000
    query_node_ids = []

    for i in range(0, len(sampled_indices), batch_size):
        batch_indices = sampled_indices[i : i + batch_size]

        # Read only these specific rows
        batch_df = scanner.take(batch_indices).to_pandas()
        query_node_ids.extend(batch_df["node_ids"].tolist())

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

    return results.iloc[0]["embeddings"]


def generate_predictions_lazy(
    query_node_ids: list[str],
    table: lancedb.table.Table,
    n_retrieved: int,
) -> pd.DataFrame:
    """
    Retrieve top-n_retrieved similar items.

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


if __name__ == "__main__":
    N_RETRIEVED = 10000
    K_VALUES_TO_EVAL = [10, 20, 50, 100]
    TABLE_NAME = "embedding_table"
    RUN_ID = "algo_training_graph_embeddings_20251021T070003"
    PARQUET_PATH = f"gs://{ML_BUCKET_TEMP}/algo_training_{ENV_SHORT_NAME}/{RUN_ID}/embeddings.parquet"

    # Metadata configuration
    METADATA_PARQUET_PATH = f"gs://{ML_BUCKET_TEMP}/algo_training_{ENV_SHORT_NAME}/{RUN_ID}/raw_input/data-*.parquet"
    METADATA_COLUMNS = ["item_id", "gtl_id", "artist_id"]

    DISPLAY_SUMMARY = False
    N_SAMPLES = 1000
    RELEVANCE_THRESHOLDS = [0.5, 0.7, 0.9]

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
        left_on="node_ids",
        right_on="item_id",
    )

    # ==================================================================================
    # Step 5: Compute ALL scores in one merged lazy pass (GTL + Artist + Full)
    logger.info("=" * 50)
    logger.info("STEP 5: Computing ALL Scores (Single Merged Pass)")
    logger.info("=" * 50)

    df_results = compute_all_scores_lazy(
        augmented_results=df_results,
        table=embedding_table,
        artist_col_query="query_artist_id",
        artist_col_retrieved="retrieved_artist_id",
        batch_size=1000,
        force_artist_weight=True,
    )

    # ==================================================================================
    # Step 6: Compute Recommendation Metrics
    logger.info("=" * 50)
    logger.info("STEP 6: Computing Recommendation Metrics")
    logger.info("=" * 50)
    metrics, df_results = compute_retrieval_metrics(
        retrieval_results=df_results,
        k_values=[10, 20, 50, 100],
        score_cols="full_score",
        relevancy_thresholds=[0.85, 0.9, 0.95],
        training_params={
            "model": "graph_embeddings",
            "n_samples": N_SAMPLES,
            "N_RETRIEVED": N_RETRIEVED,
        },
    )

    logger.info("=" * 50)
    logger.info("Evaluation Complete!")
    logger.info("=" * 50)

    logger.info("=" * 50)
    logger.info("SUMMARY")
    logger.info("=" * 50)
    # Access metrics
    logger.info("=" * 50)
    logger.info(f"RECALL:\n{metrics['recall']['full_score']}")
    logger.info("=" * 50)
    logger.info(f"NDCG:\n{metrics['ndcg']['full_score']}")
    logger.info("=" * 50)
    logger.info(f"PRECISION:\n{metrics['precision']['full_score']}")
    logger.info("=" * 50)

    # # Save results
    # output_path = RESULTS_DIR / f"retrieval_results_{RUN_ID}.parquet"
    # retrieval_results.to_parquet(output_path, index=False)
    # logger.info(f"Saved results to: {output_path}")
