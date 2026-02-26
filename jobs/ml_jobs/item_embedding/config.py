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

    try:
        with open(config_path, mode="r", encoding="utf-8") as config_file:
            config = yaml.safe_load(config_file)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file: {e}")
        raise

    if not isinstance(config, dict):
        raise ValueError(
            f"Config file must contain a YAML mapping, got {type(config).__name__}"
        )

    missing_keys = REQUIRED_CONFIG_KEYS - config.keys()
    if missing_keys:
        raise ValueError(
            f"Config file is missing required keys: {', '.join(sorted(missing_keys))}. "
            f"Found keys: {', '.join(sorted(config.keys()))}"
        )

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
    raw_vectors = config.get("vectors", [])
    if not isinstance(raw_vectors, list):
        raise ValueError(f"'vectors' must be a list, got {type(raw_vectors).__name__}")

    vectors = [Vector(**vector_config) for vector_config in raw_vectors]

    if not vectors:
        raise ValueError("No vectors configured in the configuration file")

    return vectors
