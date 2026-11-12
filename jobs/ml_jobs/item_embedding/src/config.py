import pathlib
from string import Formatter
from typing import Optional

import yaml
from loguru import logger
from pydantic import BaseModel, field_validator, model_validator

# configs/ lives at the job root, one level up from this file's src/ package.
CONFIGS_PATH = pathlib.Path(__file__).parent.parent / "configs"

# Each config file describes exactly one vector; these are the keys it cannot
# be built without.
REQUIRED_CONFIG_KEYS = {"name", "features", "encoder_name"}


def _template_line_fields(line: str) -> list[str]:
    """Field names referenced by the ``{placeholder}``s on one template line."""
    return [name for _, name, _, _ in Formatter().parse(line) if name is not None]


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

    @model_validator(mode="after")
    def _validate_feature_references(self) -> "Vector":
        """Every key in ``preprocessors``/``labels``, and every ``{field}`` in
        ``prompt_template``, must reference a declared feature -- checked here
        (once, at config-load time) rather than left to surface later as a
        bare ``KeyError`` deep in pandas, or only on a chunk that happens to
        have rows to render.
        """
        features = set(self.features)

        bad_preprocessors = set(self.preprocessors) - features
        if bad_preprocessors:
            raise ValueError(
                f"Vector '{self.name}': preprocessors key(s) not in features: "
                f"{sorted(bad_preprocessors)}"
            )

        bad_labels = set(self.labels) - features
        if bad_labels:
            raise ValueError(
                f"Vector '{self.name}': labels key(s) not in features: "
                f"{sorted(bad_labels)}"
            )

        if self.prompt_template is not None:
            template_fields = {
                field
                for line in self.prompt_template.split("\n")
                for field in _template_line_fields(line)
            }
            bad_fields = template_fields - features
            if bad_fields:
                raise ValueError(
                    f"Vector '{self.name}': prompt_template references "
                    f"field(s) not in features: {sorted(bad_fields)}"
                )

        return self


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
            key, references an unknown preprocessor, or has a
            preprocessors/labels/prompt_template key that isn't in features.
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
