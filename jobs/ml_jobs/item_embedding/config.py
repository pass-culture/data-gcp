import pathlib
from typing import Optional

import yaml
from loguru import logger
from pydantic import BaseModel

CONFIGS_PATH = pathlib.Path(__file__).parent / "configs"

REQUIRED_CONFIG_KEYS = {"vectors"}


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
        ValueError: If config is missing required keys
    """
    config_path = CONFIGS_PATH / f"{config_file_name}.yaml"
    logger.info(f"Loading config from: {config_path}")

    with open(config_path, mode="r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    missing_keys = REQUIRED_CONFIG_KEYS - config.keys()
    if missing_keys:
        raise ValueError(f"Config is missing required keys: {missing_keys}")

    return config


def parse_vectors(config: dict) -> list[Vector]:
    """Parse vector configurations from config dictionary.

    Args:
        config: Configuration dictionary containing a 'vectors' key

    Returns:
        List of Vector objects

    Raises:
        ValueError: If no vectors are configured or vector config is invalid
    """
    raw_vectors = config.get("vectors")

    if raw_vectors is None or (isinstance(raw_vectors, list) and not raw_vectors):
        raise ValueError("No vectors configured")

    if not isinstance(raw_vectors, list):
        raise ValueError("vectors config must be a list")

    vectors = [Vector(**vector_config) for vector_config in raw_vectors]
    return vectors
