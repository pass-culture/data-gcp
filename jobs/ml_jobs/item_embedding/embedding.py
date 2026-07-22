import numpy as np
import pandas as pd
from config import Vector
from constants import BATCH_SIZE
from loguru import logger
from sentence_transformers import SentenceTransformer


def _build_prompts(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build text prompts for all rows using vectorized operations.

    Concatenates non-null feature values as ``"label : value"`` pairs
    separated by newlines.
    The label defaults to the column name unless overridden in vector.labels.
    Features with null values are omitted entirely.
    Items with all-null features will have an empty prompt string and are logged as a warning.
    They are skipped entirely by the caller and excluded from the output (no null vectors).

    Args:
        df: DataFrame with item metadata
        vector: Vector configuration

    Returns:
        List of formatted prompt strings, one per row
    """
    parts = []
    for feature in vector.features:
        label = vector.labels.get(feature, feature)
        mask = df[feature].notna() & (df[feature].astype(str).str.strip() != "")
        formatted = pd.Series("", index=df.index)
        formatted[mask] = label + " : " + df[feature][mask].astype(str)
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

    Items with no metadata to embed for one or more vectors (all-null features,
    i.e. an empty prompt) are skipped entirely and logged, so every vector
    column in the returned DataFrame is fully populated with real embeddings —
    the output never contains null vectors.

    Args:
        df: DataFrame with item metadata (must contain 'item_id', 'content_hash' and all feature columns)
        vectors: Vector configurations
        encoders: Pre-loaded encoders keyed by encoder name
        pools: Pre-started multi-process pools keyed by encoder name. When a
            pool exists for a vector's encoder it is reused; otherwise the
            vector is encoded on a single device.

    Returns:
        DataFrame with 'item_id', 'content_hash', and one column per vector.
        Each vector column contains an embedding array for every row; items
        lacking metadata for any vector are excluded from the result.
    """
    pools = pools or {}
    logger.info(f"Embedding {len(df)} items")

    # Cache prompts by (features, labels) to avoid re-building identical text
    prompts_cache: dict[
        tuple[tuple[str, ...], tuple[tuple[str, str], ...]], list[str]
    ] = {}

    prompts_by_vector: dict[str, list[str]] = {}
    for vector in vectors:
        # Prompt text depends on both the columns and their labels.
        features_key = (tuple(vector.features), tuple(sorted(vector.labels.items())))
        if features_key not in prompts_cache:
            prompts_cache[features_key] = _build_prompts(df, vector)
        else:
            logger.info(
                f"Vector '{vector.name}': reusing cached prompts "
                f"(same features as a previous vector)"
            )
        prompts_by_vector[vector.name] = prompts_cache[features_key]

    # Keep only items that have metadata for *every* vector (non-empty prompt),
    # so no vector column ends up with a null embedding. An empty prompt means
    # all of that vector's features are null for the item.
    keep_mask = np.ones(len(df), dtype=bool)
    for prompts in prompts_by_vector.values():
        keep_mask &= np.array([bool(prompt) for prompt in prompts], dtype=bool)

    dropped_items = df.loc[~keep_mask, "item_id"].tolist()
    if dropped_items:
        logger.warning(
            f"Skipping {len(dropped_items)} item(s) with no metadata to embed; "
            f"they are excluded from the output. Item ids:\n{dropped_items}"
        )
    if not keep_mask.any():
        logger.warning("No item has metadata to embed; returning an empty result.")

    df_embeddings = df.loc[keep_mask, ["item_id", "content_hash"]].reset_index(
        drop=True
    )

    for vector in vectors:
        prompts = [
            prompt
            for prompt, keep in zip(prompts_by_vector[vector.name], keep_mask)
            if keep
        ]
        if not prompts:
            df_embeddings[vector.name] = pd.Series(dtype=object)
            continue

        vector_embeddings = embed_vector(
            vector,
            encoders[vector.encoder_name],
            prompts=prompts,
            pool=pools.get(vector.encoder_name),
        )
        # Positional assignment: prompts and embeddings align with the kept rows.
        df_embeddings[vector.name] = vector_embeddings.tolist()

    return df_embeddings
