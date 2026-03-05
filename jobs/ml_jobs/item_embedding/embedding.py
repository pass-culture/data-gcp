
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from config import Vector
from constants import HF_TOKEN_SECRET_NAME
from gcp_secrets import get_secret
from loguru import logger
from sentence_transformers import SentenceTransformer


def _build_prompts(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build text prompts for all rows using vectorized operations.

    Concatenates non-null feature values as ``"feature : value"`` pairs
    separated by newlines. Features with null values are omitted entirely.
    Rows where all features are null produce an empty string and are logged
    as a warning.

    Args:
        df: DataFrame with item metadata
        vector: Vector configuration

    Returns:
        List of formatted prompt strings, one per row
    """
    # Format each feature as "feature : value"; omit entirely if null or empty
    parts = []
    for feature in vector.features:
        mask = df[feature].notna() & (df[feature].astype(str).str.strip() != "")
        formatted = pd.Series("", index=df.index)
        formatted[mask] = feature + " : " + df[feature][mask].astype(str)
        parts.append(formatted)

    # Join non-null parts per row with a newline (null features are omitted)
    combined = pd.concat(parts, axis=1).agg(
        lambda row: "\n".join(filter(None, row)), axis=1
    )

    empty_count = (combined == "").sum()
    empty_items = df[(combined == "")].index
    if empty_count > 0:
        logger.warning(
            f"Vector '{vector.name}': {empty_count} rows have all-null features"
            f"{list(empty_items)}"
        )

    return combined.tolist()


def load_encoders(vectors: list[Vector]) -> dict[str, SentenceTransformer]:
    """Load each unique encoder once.

    Args:
        vectors: List of vector configurations

    Returns:
        Dictionary mapping encoder names to loaded SentenceTransformer instances
    """
    unique_encoder_names = {v.encoder_name for v in vectors}
    token = get_secret(HF_TOKEN_SECRET_NAME)
    encoders = {}
    for name in unique_encoder_names:
        logger.info(f"Loading encoder: {name}")
        encoders[name] = SentenceTransformer(name, token=token)
    return encoders


def _get_gpu_count() -> int:
    """Return the number of available CUDA GPUs."""
    return torch.cuda.device_count() if torch.cuda.is_available() else 0


def _embed_vector(
    vector: Vector,
    encoder: SentenceTransformer,
    df: pd.DataFrame,
    batch_size: int = 100,
    gpu_count: int = 0,
    prompts: list[str] | None = None,
) -> np.ndarray:
    """Generate embeddings for a single vector configuration.

    Automatically uses multi-GPU encoding when more than one GPU is
    available and the dataset is large enough to justify the overhead.

    Args:
        vector: Vector configuration
        encoder: Pre-loaded SentenceTransformer encoder
        df: DataFrame with item metadata
        batch_size: Batch size passed to encoder.encode()
        gpu_count: Number of available GPUs (avoids re-detection per vector)
        prompts: Pre-built prompts; if None, they are built from df

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing vector: {vector.name}")

    if prompts is None:
        prompts = _build_prompts(df, vector)

    use_multi_gpu = gpu_count > 1

    if use_multi_gpu:
        logger.info(
            f"Using multi-GPU encoding with {gpu_count} GPUs for {len(prompts)} prompts"
        )
        pool = encoder.start_multi_process_pool()
        try:
            embeddings = encoder.encode(
                prompts,
                convert_to_numpy=True,
                show_progress_bar=True,
                pool=pool,
                batch_size=batch_size,
                prompt_name=vector.prompt_name,
            )
        finally:
            encoder.stop_multi_process_pool(pool)
    else:
        logger.info(f"Using single-device encoding ({encoder.device})")
        embeddings = encoder.encode(
            prompts,
            convert_to_numpy=True,
            show_progress_bar=True,
            batch_size=batch_size,
            prompt_name=vector.prompt_name,
        )

    logger.info(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")
    return embeddings


def _embed_batch(
    df: pd.DataFrame,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    batch_size: int,
    gpu_count: int,
) -> pa.Table:
    """Compute all vector embeddings for a single batch and return an Arrow Table.

    Args:
        df: Batch DataFrame (must contain 'item_id' and all feature columns)
        vectors: Vector configurations
        encoders: Pre-loaded encoders keyed by encoder name
        batch_size: Batch size for encoder.encode()
        gpu_count: Number of available GPUs

    Returns:
        Arrow Table with 'item_id' and 'content_hash' plus one FixedSizeList column per vector
    """
    # Cache prompts by feature tuple to avoid re-building identical text
    prompts_cache: dict[tuple[str, ...], list[str]] = {}

    columns: dict[str, pa.Array] = {
        "item_id": pa.array(df["item_id"].values),
        "content_hash": pa.array(df["content_hash"].values),
    }

    for vector in vectors:
        features_key = tuple(vector.features)
        if features_key not in prompts_cache:
            prompts_cache[features_key] = _build_prompts(df, vector)
        else:
            logger.info(
                f"Vector '{vector.name}': reusing cached prompts "
                f"(same features as a previous vector)"
            )

        vector_embeddings = _embed_vector(
            vector,
            encoders[vector.encoder_name],
            df,
            batch_size,
            gpu_count,
            prompts=prompts_cache[features_key],
        )

        # Store as Arrow FixedSizeListArray — keeps data in a single
        # contiguous buffer instead of millions of Python list objects.
        flat_arrow = pa.array(vector_embeddings.ravel())
        arrow_list = pa.FixedSizeListArray.from_arrays(
            flat_arrow, list_size=vector_embeddings.shape[1]
        )
        columns[vector.name] = arrow_list
        del vector_embeddings, flat_arrow  # free numpy memory

    return pa.table(columns)


def embed_parquet_file(
    pf: pq.ParquetFile,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    output_file_schema: pa.Schema,
    output_path: str,
    batch_size: int = 100,
) -> str:
    """Stream-embed a ParquetFile and write results to a local temp Parquet.

    Reads *pf* in batches via ``iter_batches``, computes all vector
    embeddings for each batch, and incrementally writes them to a
    temporary Parquet file using ``pq.ParquetWriter``.

    Args:
        pf: ParquetFile with item metadata (must contain 'item_id')
        vectors: List of vector configurations
        encoders: Dictionary mapping encoder names to loaded SentenceTransformer instances
        batch_size: Batch size for encoding **and** for ``iter_batches``

    Returns:
        Absolute path to the local temporary Parquet file containing the
        embeddings.  The caller is responsible for uploading and cleaning
        up this file.
    """
    gpu_count = _get_gpu_count()
    logger.info(f"Detected {gpu_count} GPU(s)")
    writer = pq.ParquetWriter(output_path, schema=output_file_schema)

    total_rows = 0
    for record_batch in pf.iter_batches(batch_size=batch_size):
        df = record_batch.to_pandas()
        logger.info(f"Processing batch with {len(df)} items")

        table = _embed_batch(df, vectors, encoders, batch_size, gpu_count)

        if writer is None:
            writer = pq.ParquetWriter(tmp_path, table.schema)

        writer.write_table(table)
        total_rows += table.num_rows

        del df, table  # free memory before next batch

    if writer is not None:
        writer.close()

    logger.info(f"Wrote {total_rows} rows to local temp file: {tmp_path}")

    return tmp_path
