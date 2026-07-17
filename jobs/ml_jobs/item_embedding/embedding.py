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
    They will later receive a ``None`` embedding without calling the model.

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
    Empty prompts are skipped and receive ``None`` for that vector.

    Args:
        df: DataFrame with item metadata (must contain 'item_id', 'content_hash' and all feature columns)
        vectors: Vector configurations
        encoders: Pre-loaded encoders keyed by encoder name
        pools: Pre-started multi-process pools keyed by encoder name. When a
            pool exists for a vector's encoder it is reused; otherwise the
            vector is encoded on a single device.

    Returns:
        DataFrame with 'item_id', 'content_hash', and one column per vector.
        Each vector column contains embedding arrays. Rows whose features are
        all null get ``None`` for that vector instead of an embedding.
    """
    pools = pools or {}
    logger.info(f"Embedding {len(df)} items")

    # Cache prompts by (features, labels) to avoid re-building identical text
    prompts_cache: dict[
        tuple[tuple[str, ...], tuple[tuple[str, str], ...]], list[str]
    ] = {}

    df_embeddings = df[["item_id", "content_hash"]].copy()

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

        prompts = prompts_cache[features_key]
        # Skip all-null rows (empty prompt): don't embed the bare instruction.
        keep_mask = [bool(prompt) for prompt in prompts]
        non_empty_prompts = [prompt for prompt, keep in zip(prompts, keep_mask) if keep]

        if non_empty_prompts:
            vector_embeddings = embed_vector(
                vector,
                encoders[vector.encoder_name],
                prompts=non_empty_prompts,
                pool=pools.get(vector.encoder_name),
            )
            embeddings_iter = iter(vector_embeddings.tolist())
            column = [next(embeddings_iter) if keep else None for keep in keep_mask]
        else:
            logger.warning(
                f"Vector '{vector.name}': all rows have empty prompts; "
                f"writing None for every item."
            )
            column = [None] * len(prompts)

        df_embeddings[vector.name] = column

    return df_embeddings
