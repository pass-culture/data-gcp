import numpy as np
import pandas as pd
import torch
from config import Vector
from constants import HF_TOKEN_SECRET_NAME
from loguru import logger
from sentence_transformers import SentenceTransformer
from utils import get_secret

MULTI_GPU_MIN_ITEMS = 100_000


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
    separated by spaces. Rows where all features are null produce an
    empty string and are logged as a warning.

    Args:
        df: DataFrame with item metadata
        vector: Vector configuration

    Returns:
        List of formatted prompt strings, one per row
    """
    parts = []
    for feature in vector.features:
        col = df[feature]
        mask = col.notna()
        part = pd.Series("", index=df.index)
        part[mask] = feature + " : " + col[mask].astype(str)
        parts.append(part)

    # Concatenate parts, collapse multiple spaces from empty parts, strip edges
    combined = parts[0].str.cat(parts[1:], sep=" ")
    combined = combined.str.replace(r" {2,}", " ", regex=True).str.strip()

    empty_count = (combined == "").sum()
    if empty_count > 0:
        logger.warning(
            f"Vector '{vector.name}': {empty_count} rows have all-null features "
            f"and will produce empty prompts"
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

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing vector: {vector.name}")

    prompts = _build_prompts(df_items_metadata, vector)

    use_multi_gpu = gpu_count > 1 and len(prompts) >= MULTI_GPU_MIN_ITEMS

    if use_multi_gpu:
        logger.info(
            f"Using multi-GPU encoding with {gpu_count} GPUs "
            f"for {len(prompts)} prompts"
        )
        pool = encoder.start_multi_process_pool()
        try:
            embeddings = encoder.encode_multi_process(
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
        if gpu_count <= 1:
            logger.info(f"Using single-device encoding ({encoder.device})")
        else:
            logger.info(
                f"{gpu_count} GPUs available but only {len(prompts)} prompts "
                f"(< {MULTI_GPU_MIN_ITEMS}), using single-device encoding"
            )
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

    Raises:
        ValueError: If DataFrame is empty or missing 'item_id'
    """
    if df_items_metadata.empty:
        raise ValueError("Empty metadata DataFrame provided")

    if "item_id" not in df_items_metadata.columns:
        raise ValueError(
            "Input DataFrame must contain an 'item_id' column. "
            f"Found columns: {df_items_metadata.columns.tolist()}"
        )

    # Validate all features upfront before loading any encoder
    for vector in vectors:
        _validate_required_features(df_items_metadata, vector.features)

    # Detect GPU count once
    gpu_count = _get_gpu_count()
    logger.info(f"Detected {gpu_count} GPU(s)")

    # Start with item_id column
    df_embeddings = pd.DataFrame({"item_id": df_items_metadata["item_id"].values})

    encoders = _load_encoders(vectors)

    for vector in vectors:
        vector_embeddings = _embed_vector(
            vector,
            encoders[vector.encoder_name],
            df_items_metadata,
            batch_size,
            gpu_count,
        )

        # Store as flat (dim,) arrays per row
        df_embeddings[vector.name] = list(vector_embeddings)

    logger.info(f"Final embeddings dataframe columns: {df_embeddings.columns.tolist()}")
    logger.info(f"Number of items: {df_embeddings.shape[0]}")
    return df_embeddings
