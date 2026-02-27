import numpy as np
import pandas as pd
import torch
from config import Vector
from constants import HF_TOKEN_SECRET_NAME
from loguru import logger
from sentence_transformers import SentenceTransformer
from utils import get_secret


def _validate_required_features(df: pd.DataFrame, features: list[str]) -> None:
    """Validate that all required features are present in the DataFrame.

    Args:
        df: DataFrame to check for required features.
        features: List of required feature column names.

    Raises:
        ValueError: If any required feature is missing from the DataFrame.
    """
    missing_features = [f for f in features if f not in df.columns]
    if missing_features:
        raise ValueError(f"Missing required features: {', '.join(missing_features)}")


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


def _load_encoders(vectors: list[Vector]) -> dict[str, SentenceTransformer]:
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
    df_items_metadata: pd.DataFrame,
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
        df_items_metadata: DataFrame with item metadata
        batch_size: Batch size passed to encoder.encode()
        gpu_count: Number of available GPUs (avoids re-detection per vector)
        prompts: Pre-built prompts; if None, they are built from df_items_metadata

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing vector: {vector.name}")

    if prompts is None:
        prompts = _build_prompts(df_items_metadata, vector)

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


def embed_all_vectors(
    df_items_metadata: pd.DataFrame, vectors: list[Vector], batch_size: int = 100
) -> pd.DataFrame:
    """Generate embeddings for all configured vectors.

    Args:
        df_items_metadata: DataFrame with item metadata (must contain 'item_id')
        vectors: List of vector configurations
        batch_size: Batch size for encoding

    Returns:
        DataFrame with item_id and one embedding column per vector
    """

    # Validate all features upfront before loading any encoder
    for vector in vectors:
        _validate_required_features(df_items_metadata, vector.features)

    # Detect GPU count once
    gpu_count = _get_gpu_count()
    logger.info(f"Detected {gpu_count} GPU(s)")

    # Start with item_id column
    df_embeddings = pd.DataFrame({"item_id": df_items_metadata["item_id"].values})

    encoders = _load_encoders(vectors)

    # Cache prompts by feature list — two vectors sharing the same features
    # produce identical prompt text regardless of their prompt_name
    prompts_cache: dict[tuple[str, ...], list[str]] = {}

    for vector in vectors:
        features_key = tuple(vector.features)
        if features_key not in prompts_cache:
            prompts_cache[features_key] = _build_prompts(df_items_metadata, vector)
        else:
            logger.info(
                f"Vector '{vector.name}': reusing cached prompts "
                f"(same features as a previous vector)"
            )

        vector_embeddings = _embed_vector(
            vector,
            encoders[vector.encoder_name],
            df_items_metadata,
            batch_size,
            gpu_count,
            prompts=prompts_cache[features_key],
        )

        # Store as Python lists so PyArrow writes list<float64>
        df_embeddings[vector.name] = [arr.tolist() for arr in vector_embeddings]

    logger.info(f"Final embeddings dataframe columns: {df_embeddings.columns.tolist()}")
    logger.info(f"Number of items: {df_embeddings.shape[0]}")
    return df_embeddings
