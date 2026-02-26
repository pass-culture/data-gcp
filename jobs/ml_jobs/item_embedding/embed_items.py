from typing import Generator, Optional

import numpy as np
import pandas as pd
import yaml
from constants import HF_TOKEN_SECRET_NAME
from loguru import logger
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from utils import get_secret

CONFIGS_PATH = "configs"


class Vector(BaseModel):
    name: str
    features: list[str]
    encoder_name: str
    prompt_name: Optional[str] = None


def load_config(config_file_name: str) -> dict:
    """Load YAML configuration file.

    Args:
        config_file_name: Name of the config file (without .yaml extension)

    Returns:
        Dictionary containing configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid YAML
    """
    config_path = f"{CONFIGS_PATH}/{config_file_name}.yaml"
    logger.info(f"Loading config from: {config_path}")

    try:
        with open(config_path, mode="r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file: {e}")
        raise


def parse_vectors(config: dict) -> list[Vector]:
    """Parse vector configurations from config dictionary.

    Args:
        config: Configuration dictionary

    Returns:
        List of Vector objects

    Raises:
        ValueError: If no vectors are configured
    """
    vectors = [Vector(**vector_config) for vector_config in config.get("vectors", [])]

    if not vectors:
        logger.error("No vectors configured in the configuration file")
        raise ValueError("No vectors configured in the configuration file")
    return vectors


def _assert_required_features(df: pd.DataFrame, features: list) -> None:
    """
    Asserts that all required features are present in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to check for required features.
        features (list): List of required feature names.

    Raises:
        ValueError: If any required feature is missing from the DataFrame.
    """
    missing_features = [feature for feature in features if feature not in df.columns]
    if missing_features:
        raise ValueError(f"Missing required features: {', '.join(missing_features)}")


def _build_prompt(row: pd.Series, vector: Vector) -> str:
    """Build text prompt from row features.

    Args:
        row: DataFrame row
        vector: Vector configuration

    Returns:
        Formatted prompt string
    """
    parts = [
        f"{feature} : {row[feature]}"
        for feature in vector.features
        if pd.notna(row[feature])
    ]
    return " ".join(parts) if parts else ""


def _iterate_df(
    df: pd.DataFrame, batch_size: int
) -> Generator[pd.DataFrame, None, None]:
    """Lazily iterate over DataFrame in batches.

    Args:
        df: DataFrame to iterate
        batch_size: Number of rows per batch

    Yields:
        DataFrame batches
    """
    for start in range(0, len(df), batch_size):
        yield df.iloc[start : start + batch_size]


def _embed_vector(
    vector: Vector, df_items_metadata: pd.DataFrame, batch_size: int = 100
) -> np.ndarray:
    logger.info(f"Processing vector: {vector.name}")

    _assert_required_features(df_items_metadata, vector.features)

    encoder = SentenceTransformer(
        vector.encoder_name, token=get_secret(HF_TOKEN_SECRET_NAME)
    )
    logger.info(f"Loaded encoder: {vector.encoder_name}")

    # Process in batches
    vector_embeddings = []
    num_batches = (df_items_metadata.shape[0] + batch_size - 1) // batch_size

    for df_subset in tqdm(
        _iterate_df(df_items_metadata, batch_size),
        desc=f"Embedding {vector.name}",
        total=num_batches,
    ):
        # Create prompts for this batch
        prompts = df_subset.apply(
            lambda row: _build_prompt(row, vector), axis=1
        ).tolist()

        embeddings = encoder.encode(
            prompts,
            convert_to_numpy=True,
            show_progress_bar=False,
            prompt_name=vector.prompt_name,
        )
        vector_embeddings.append(embeddings)
    # Concatenate all batches for this vector
    vector_embeddings = np.vstack(vector_embeddings)
    logger.info(
        f"  Generated {len(vector_embeddings)} embeddings with shape {vector_embeddings.shape}"
    )
    return vector_embeddings


def embed_all_vectors(
    df_items_metadata: pd.DataFrame, vectors: list[Vector], batch_size: int = 100
) -> pd.DataFrame:
    """Generate embeddings for all configured vectors.

    Args:
        df_items_metadata: DataFrame with item metadata
        vectors: List of vector configurations
        batch_size: Batch size for encoding

    Returns:
        DataFrame with item_id and embedding columns for each vector
    """
    if df_items_metadata.empty:
        raise ValueError("Empty metadata DataFrame provided")

    # Start with item_id column
    df_embeddings = pd.DataFrame({"item_id": df_items_metadata["item_id"].values})

    for vector in vectors:
        vector_embeddings = _embed_vector(vector, df_items_metadata, batch_size)

        # Add embeddings as new column
        df_embeddings[vector.name] = list(vector_embeddings)

    logger.info(f"Final embeddings dataframe columns: {df_embeddings.columns.tolist()}")
    logger.info(f"Number of items: {df_embeddings.shape[0]}")
    return df_embeddings
