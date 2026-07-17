import numpy as np
import pandas as pd
from config import Vector
from constants import BATCH_SIZE
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
            f"Vector '{vector.name}': {empty_count} rows have all-null features.\n Empty items are:\n {list(empty_items)}"
        )

    return combined.tolist()


def embed_vector(
    vector: Vector,
    encoder: SentenceTransformer,
    prompts: list[str],
    pool: object = None,
) -> np.ndarray:
    """Generate embeddings for a single vector configuration.

    Uses multi-GPU encoding when a pre-started ``pool`` is provided, otherwise
    falls back to single-device encoding.

    Args:
        vector: Vector configuration
        encoder: Pre-loaded SentenceTransformer encoder
        prompts: Pre-built prompts
        pool: Pre-started multi-process pool, or ``None`` for single-device

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing vector '{vector.name}' (batch_size={BATCH_SIZE})")
    encode_kwargs = {
        "convert_to_numpy": True,
        "show_progress_bar": False,
        "batch_size": BATCH_SIZE,
        "prompt_name": vector.prompt_name,
        # L2-normalize so cosine similarity, dot-product indexes and
        # Euclidean/k-means clustering all behave consistently downstream.
        "normalize_embeddings": True,
    }

    if pool is not None:
        logger.info(f"Using multi-GPU encoding for {len(prompts)} prompts")
        embeddings = encoder.encode(prompts, pool=pool, **encode_kwargs)
    else:
        logger.info(f"Using single-device encoding ({encoder.device})")
        embeddings = encoder.encode(prompts, **encode_kwargs)

    logger.info(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")
    return embeddings


def embed_dataframe(
    df: pd.DataFrame,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    pools: dict[str, object] = None,
) -> pd.DataFrame:
    """Compute all vector embeddings for a dataframe.

    Args:
        df: DataFrame with item metadata (must contain 'item_id', 'content_hash' and all feature columns)
        vectors: Vector configurations
        encoders: Pre-loaded encoders keyed by encoder name
        pools: Pre-started multi-process pools keyed by encoder name. When a
            pool exists for a vector's encoder it is reused; otherwise the
            vector is encoded on a single device.

    Returns:
        DataFrame with 'item_id', 'content_hash', and one column per vector.
        Each vector column contains embedding arrays.
    """
    pools = pools or {}
    logger.info(f"Embedding {len(df)} items")

    # Cache prompts by feature tuple to avoid re-building identical text
    prompts_cache: dict[tuple[str, ...], list[str]] = {}

    df_embeddings = df[["item_id", "content_hash"]].copy()

    for vector in vectors:
        features_key = tuple(vector.features)
        if features_key not in prompts_cache:
            prompts_cache[features_key] = _build_prompts(df, vector)
        else:
            logger.info(
                f"Vector '{vector.name}': reusing cached prompts "
                f"(same features as a previous vector)"
            )

        vector_embeddings = embed_vector(
            vector,
            encoders[vector.encoder_name],
            prompts=prompts_cache[features_key],
            pool=pools.get(vector.encoder_name),
        )

        df_embeddings[vector.name] = vector_embeddings.tolist()

    return df_embeddings
