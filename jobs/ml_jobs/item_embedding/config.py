import pathlib
from typing import Optional

import yaml
from loguru import logger
from pydantic import BaseModel, field_validator, model_validator

CONFIGS_PATH = pathlib.Path(__file__).parent / "configs"

REQUIRED_CONFIG_KEYS = {"vectors"}


class FilterCondition(BaseModel):
    """A single-column match, matched on `column` by exactly one of:
    - `values`: item is kept if `column` value is in this list
      (e.g. column="category_id", values=["CINEMA"] to scope to movies).
    - `prefix`: item is kept if `column` value starts with this string
      (e.g. column="item_id", prefix="product" to match ``LEFT(item_id,
      LEN('product')) = 'product'``).

    `column` is a free string (not an enum) so any catalog column can be
    used, keeping the condition fully variabilized via config.
    """

    column: str
    values: Optional[list[str]] = None
    prefix: Optional[str] = None

    @model_validator(mode="after")
    def _check_exactly_one_match_mode(self) -> "FilterCondition":
        if (self.values is None) == (self.prefix is None):
            raise ValueError(
                "FilterCondition requires exactly one of `values` or `prefix`"
            )
        return self


class FilterGroup(BaseModel):
    """A set of `FilterCondition`s combined with AND."""

    conditions: list[FilterCondition]


class CategoryFilter(BaseModel):
    """Restricts a vector to a subset of items via two composable layers:
    - `all_of`: conditions combined with AND, applied unconditionally
      (e.g. an item_id prefix that must always hold).
    - `any_of`: groups of conditions; an item matches if it satisfies at
      least one group (OR across groups), where each group's own conditions
      are combined with AND (e.g. category X, or category Y AND subcategory
      in [...]).

    Both layers are ANDed together when both are set. At least one of
    `all_of`/`any_of` must be non-empty, otherwise the filter does nothing.
    """

    all_of: list[FilterCondition] = []
    any_of: list[FilterGroup] = []

    @model_validator(mode="after")
    def _check_not_empty(self) -> "CategoryFilter":
        if not self.all_of and not self.any_of:
            raise ValueError(
                "CategoryFilter requires at least one of `all_of` or `any_of`"
            )
        return self


class Vector(BaseModel):
    name: str
    features: list[str]
    encoder_name: str
    prompt_name: Optional[str] = None
    labels: dict[str, str] = {}
    category_filter: Optional[CategoryFilter] = None
    prompt_template: Optional[str] = None
    preprocessors: dict[str, str] = {}

    @field_validator("preprocessors")
    @classmethod
    def _validate_preprocessors(cls, v: dict[str, str]) -> dict[str, str]:
        from preprocessing import PREPROCESSORS

        unknown = set(v.values()) - PREPROCESSORS.keys()
        if unknown:
            raise ValueError(
                f"Unknown preprocessor(s): {sorted(unknown)}. "
                f"Registered: {sorted(PREPROCESSORS)}"
            )
        return v


def _load_config(config_file_name: str) -> dict:
    """Load YAML configuration file of vectors to embed.

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


def parse_vectors(config_filename: str) -> list[Vector]:
    """Parse vector configurations from config dictionary.

    Args:
        config_filename: Configuration filename (without .yaml extension) to load and parse vector configurations from.

    Returns:
        List of Vector objects

    Raises:
        ValueError: If no vectors are configured or vector config is invalid
    """
    config = _load_config(config_filename)
    raw_vectors = config.get("vectors")

    if raw_vectors is None or (isinstance(raw_vectors, list) and not raw_vectors):
        raise ValueError("No vectors configured")

    if not isinstance(raw_vectors, list):
        raise ValueError("vectors config must be a list")

    vectors = [Vector(**vector_config) for vector_config in raw_vectors]
    return vectors
