import numpy as np
import pandas as pd
import torch
from config import Vector
from constants import BATCH_SIZE, HF_TOKEN_SECRET_NAME
from gcp_secrets import get_secret
from loguru import logger
from sentence_transformers import SentenceTransformer

HF_MODEL_KWARGS = {"torch_dtype": torch.float16}


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
        encoders[name] = SentenceTransformer(
            name, token=token, model_kwargs=HF_MODEL_KWARGS
        )
    return encoders


def embed_vector(
    vector: Vector,
    encoder: SentenceTransformer,
    gpu_count: int,
    prompts: list[str],
) -> np.ndarray:
    """Generate embeddings for a single vector configuration.

    Automatically uses multi-GPU encoding when more than one GPU is
    available.

    Args:
        vector: Vector configuration
        encoder: Pre-loaded SentenceTransformer encoder
        gpu_count: Number of available GPUs (avoids re-detection per vector)
        prompts: Pre-built prompts

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing vector: {vector.name}")

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
                batch_size=BATCH_SIZE,
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
            batch_size=BATCH_SIZE,
            prompt_name=vector.prompt_name,
        )

    logger.info(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")
    return embeddings


def embed_dataframe(
    df: pd.DataFrame,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    gpu_count: int,
) -> pd.DataFrame:
    """Compute all vector embeddings for a dataframe.

    Args:
        df: DataFrame with item metadata (must contain 'item_id', 'content_hash' and all feature columns)
        vectors: Vector configurations
        encoders: Pre-loaded encoders keyed by encoder name
        gpu_count: Number of available GPUs

    Returns:
        DataFrame with 'item_id', 'content_hash', and one column per vector.
        Each vector column contains embedding arrays.
    """
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
            gpu_count,
            prompts=prompts_cache[features_key],
        )

        df_embeddings[vector.name] = vector_embeddings.tolist()

    return df_embeddings
