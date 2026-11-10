"""Loading and validating configs/*.yaml (spec section 6).

Each CLI command loads the full :class:`TopicsConfig`, then applies its own
``--option`` overrides (if any) onto the relevant section via
:func:`apply_overrides` -- only non-``None`` overrides replace a field, so an
unset CLI option keeps the YAML default.
"""

import pathlib
from typing import Literal

import yaml
from loguru import logger
from pydantic import BaseModel, Field

CONFIGS_PATH = pathlib.Path(__file__).parent.parent.parent / "configs"

REQUIRED_TOP_LEVEL_KEYS = {
    "language",
    "split_level",
    "min_corpus_items",
    "corpora",
    "vocabulary",
    "reduction",
    "clustering",
    "labelling",
}


class BoilerplateConfig(BaseModel):
    min_segment_chars: int
    min_share: float
    max_texts: int
    seed: int


class VocabularyConfig(BaseModel):
    min_description_chars: int
    boilerplate: BoilerplateConfig
    spacy_model: str
    pos: list[str]
    chunk_texts: int
    ner: bool
    generic_stop_words: list[str]
    forbidden_words: list[str]


class ReductionConfig(BaseModel):
    dim: int
    n_neighbors: int
    n_components: int
    min_dist: float
    metric: str
    seed: int


class ClusteringConfig(BaseModel):
    min_cluster_size: int | Literal["auto"]
    min_samples: int
    max_cluster_share: float
    metric: str
    cpu_fallback_jobs: int
    uuid_match_jaccard: float


class LabellingConfig(BaseModel):
    min_df: int
    max_features: int
    ngram_max: int
    reduce_frequent_words: bool
    top_n_words: int
    label_words: int
    reassign_outliers: bool
    min_shared_words: int


class TopicsConfig(BaseModel):
    language: str
    split_level: int = Field(ge=0, le=4)
    min_corpus_items: int
    corpora: str | list[str]
    vocabulary: VocabularyConfig
    reduction: ReductionConfig
    clustering: ClusteringConfig
    labelling: LabellingConfig


def load_config(config_file_name: str = "default") -> TopicsConfig:
    """Load and validate ``configs/{config_file_name}.yaml``.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        ValueError: If the file is empty, not a mapping, or missing a
            required top-level section.
    """
    config_path = CONFIGS_PATH / f"{config_file_name}.yaml"
    logger.info(f"Loading config from: {config_path}")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if config is None:
        raise ValueError(f"Config file '{config_file_name}.yaml' is empty or invalid")
    if not isinstance(config, dict):
        raise ValueError(
            f"Config file '{config_file_name}.yaml' must define a mapping, "
            f"got {type(config).__name__}"
        )
    missing_keys = REQUIRED_TOP_LEVEL_KEYS - config.keys()
    if missing_keys:
        raise ValueError(
            f"Config is missing required top-level keys: {sorted(missing_keys)}"
        )

    return TopicsConfig(**config)


def apply_overrides[ModelT: BaseModel](
    model: ModelT, overrides: dict[str, object]
) -> ModelT:
    """Return a copy of ``model`` with only its non-``None`` overrides applied.

    Lets a CLI command expose ``typer.Option(None)`` overrides for a handful
    of fields without having to special-case "was this option passed?" itself.
    """
    updates = {key: value for key, value in overrides.items() if value is not None}
    return model.model_copy(update=updates) if updates else model
