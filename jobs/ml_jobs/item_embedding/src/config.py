import pathlib
from typing import Optional

import yaml
from loguru import logger
from pydantic import BaseModel, field_validator

# configs/ lives at the job root, one level up from this file's src/ package.
CONFIGS_PATH = pathlib.Path(__file__).parent.parent / "configs"

# Each config file describes exactly one vector; these are the keys it cannot
# be built without.
REQUIRED_CONFIG_KEYS = {"name", "features", "encoder_name"}


class Vector(BaseModel):
    name: str
    features: list[str]
    encoder_name: str
    prompt_name: Optional[str] = None
    labels: dict[str, str] = {}
    prompt_template: Optional[str] = None
    preprocessors: dict[str, str] = {}

    @field_validator("preprocessors")
    @classmethod
    def _validate_preprocessors(cls, v: dict[str, str]) -> dict[str, str]:
        from src.preprocessing import PREPROCESSORS

        unknown = set(v.values()) - PREPROCESSORS.keys()
        if unknown:
            raise ValueError(
                f"Unknown preprocessor(s): {sorted(unknown)}. "
                f"Registered: {sorted(PREPROCESSORS)}"
            )
        return v


def load_vector_config(config_file_name: str) -> Vector:
    """Load and validate a single vector's YAML config.

    Args:
        config_file_name: Config file name (without .yaml extension) in
            ``configs/``. By convention this equals the vector's ``name``.

    Returns:
        The parsed, validated ``Vector``.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        yaml.YAMLError: If the file is invalid YAML.
        ValueError: If the config is empty, not a mapping, missing a required
            key, or references an unknown preprocessor.
    """
    config_path = CONFIGS_PATH / f"{config_file_name}.yaml"
    logger.info(f"Loading config from: {config_path}")

    with open(config_path, mode="r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    if config is None:
        raise ValueError(f"Config file '{config_file_name}.yaml' is empty or invalid")
    if not isinstance(config, dict):
        raise ValueError(
            f"Config file '{config_file_name}.yaml' must define a single vector "
            f"as a mapping, got {type(config).__name__}"
        )
    missing_keys = REQUIRED_CONFIG_KEYS - config.keys()
    if missing_keys:
        raise ValueError(f"Config is missing required keys: {sorted(missing_keys)}")

    return Vector(**config)
