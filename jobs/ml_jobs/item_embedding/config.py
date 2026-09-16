import pathlib
from typing import Optional

import yaml
from loguru import logger
from pydantic import BaseModel

CONFIGS_PATH = pathlib.Path(__file__).parent / "configs"

REQUIRED_CONFIG_KEYS = {"name", "features", "encoder_name"}


class Vector(BaseModel):
    name: str
    features: list[str]
    encoder_name: str
    prompt_name: Optional[str] = None
    labels: dict[str, str] = {}


def _load_config(config_file_name: str) -> dict:
    """Load the YAML configuration file describing the vector to embed.

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
    if config is None:
        raise ValueError(f"Config file '{config_file_name}.yaml' is empty or invalid")
    missing_keys = REQUIRED_CONFIG_KEYS - config.keys()
    if missing_keys:
        raise ValueError(f"Config is missing required keys: {missing_keys}")

    return config


def parse_vector(config_filename: str) -> Vector:
    """Parse the vector configuration from a config file.

    The entire config file describes a single vector.

    Args:
        config_filename: Configuration filename (without .yaml extension) to load and parse the vector configuration from.

    Returns:
        a vector object containing the parsed vector configuration

    Raises:
        ValueError: If the vector config is invalid
    """
    config = _load_config(config_filename)
    return Vector(**config)
