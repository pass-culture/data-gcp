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
    Features with null values are omitted entirely from the prompt string.
    Items with all-null features get an empty prompt string (kept in place so
    the result stays aligned row-for-row with ``df``) and are logged.

    Args:
        df: DataFrame with item metadata
        vector: Vector configuration

    Returns:
        List of formatted prompt strings, one per row (empty string for rows
        whose features are all null).
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

    empty_items = df.index[combined == ""]
    if len(empty_items) > 0:
        logger.warning(
            f"Vector '{vector.name}': {len(empty_items)} rows have all-null "
            f"features.\n Empty items are:\n {list(empty_items)}"
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
    column in the returned DataFrame is fully populated with real embeddings (non-null vectors).

    Args:
        df: DataFrame with item metadata (must contain 'item_id', 'content_hash'
            and all feature columns required by the vectors in the config file)
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

    # Put each vector's prompts in a column next to the item identifiers. Every
    # column has one entry per input row, so item_id, content_hash and all
    # prompts stay aligned on the same DataFrame row.
    prompts_df = df[["item_id", "content_hash"]].reset_index(drop=True)
    for vector in vectors:
        prompts_df[vector.name] = _build_prompts(df, vector)

    # Keep only items that have a non-empty prompt for *every* vector, so no
    # vector column ends up with a null embedding. An empty prompt means all of
    # that vector's features are null for the item.
    vector_names = [vector.name for vector in vectors]
    complete = (prompts_df[vector_names] != "").all(axis=1)

    dropped_items = prompts_df.loc[~complete, "item_id"].tolist()
    if dropped_items:
        logger.warning(
            f"Skipping {len(dropped_items)} item(s) with no metadata to embed; "
            f"they are excluded from the output. Item ids:\n{dropped_items}"
        )
    if not complete.any():
        logger.warning("No item has metadata to embed; returning an empty result.")

    prompts_df = prompts_df[complete].reset_index(drop=True)

    df_embeddings = prompts_df[["item_id", "content_hash"]].copy()
    for vector in vectors:
        prompts = prompts_df[vector.name].tolist()
        if not prompts:
            df_embeddings[vector.name] = pd.Series(dtype=object)
            continue

        vector_embeddings = embed_vector(
            vector,
            encoders[vector.encoder_name],
            prompts=prompts,
            pool=pools.get(vector.encoder_name),
        )
        df_embeddings[vector.name] = vector_embeddings.tolist()

    return df_embeddings
