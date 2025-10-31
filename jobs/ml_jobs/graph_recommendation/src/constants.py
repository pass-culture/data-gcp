import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = (PROJECT_ROOT / "data").as_posix()
RESULTS_DIR = (PROJECT_ROOT / "results").as_posix()
MLFLOW_RUN_ID_FILEPATH = (PROJECT_ROOT / "results" / "latest_run_id.txt").as_posix()

EMBEDDING_COLUMN = "embedding"
ID_COLUMN = "item_id"
GTL_ID_COLUMN = "gtl_id"
DEFAULT_METADATA_COLUMNS: Sequence[str] = (
    "gtl_label_level_1",
    "gtl_label_level_2",
    "gtl_label_level_3",
    "gtl_label_level_4",
    "artist_id",
)

MetadataKey = tuple[str, str]

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-prod")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
ML_BUCKET_TEMP = f"data-bucket-ml-temp-{ENV_SHORT_NAME}"


class BaseConfig:
    """Base class for configuration objects with JSON/dict update support."""

    def update_from_json(self, json_str: str):
        """
        Update configuration from a JSON string. Logs errors/warnings instead of raising

        Args:
            json_str: JSON string containing overrides for any config field.
        """
        try:
            updates = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON for config: {e}")
            return
        self.update_from_dict(updates)

    def update_from_dict(self, config_dict: dict):
        """Update fields from a dictionary, logging errors instead of raising."""
        if not isinstance(config_dict, dict):
            logger.error("config_dict must be a dictionary")
            return
        for k, v in config_dict.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                logger.warning(f"Ignored unknown config field: {k}")

    def to_dict(self) -> dict[str, Any]:
        """Return config as a dictionary."""
        return asdict(self)


def _default_metapath():
    paths = (
        4
        * [
            ("book", "is_artist_id", "artist_id"),
            ("artist_id", "artist_id_of", "book"),
        ]
        + 4
        * [
            ("book", "is_gtl_label_level_4", "gtl_label_level_4"),
            ("gtl_label_level_4", "gtl_label_level_4_of", "book"),
        ]
        + 3
        * [
            ("book", "is_gtl_label_level_3", "gtl_label_level_3"),
            ("gtl_label_level_3", "gtl_label_level_3_of", "book"),
        ]
        + 2
        * [
            ("book", "is_gtl_label_level_2", "gtl_label_level_2"),
            ("gtl_label_level_2", "gtl_label_level_2_of", "book"),
        ]
        + [
            ("book", "is_gtl_label_level_1", "gtl_label_level_1"),
            ("gtl_label_level_1", "gtl_label_level_1_of", "book"),
        ]
    )
    return paths


@dataclass
class DefaultTrainingConfig(BaseConfig):
    embedding_dim: int = 32
    metapath_repetitions: int = 2
    context_size: int = 10
    walks_per_node: int = 5
    num_negative_samples: int = 5
    num_epochs: int = 15
    early_stop: bool = True
    num_workers: int = 8 if sys.platform == "linux" else 0
    batch_size: int = 256
    learning_rate: float = 0.01
    metapath: list[tuple[str, str, str]] = field(default_factory=_default_metapath)


@dataclass
class DefaultEvaluationConfig(BaseConfig):
    metadata_columns: list[str] = field(
        default_factory=lambda: ["item_id", "gtl_id", "artist_id"]
    )
    n_samples: int = 100
    n_retrieved: int = 1000
    k_values: list[int] = field(default_factory=lambda: [10, 20, 50, 100])
    relevance_thresholds: list[float] = field(
        default_factory=lambda: [0.3, 0.4, 0.5, 0.6, 0.7]
    )
    ground_truth_score: str = "full_score"
    force_artist_weight: bool = False
    rebuild_index: bool = False


class InvalidConfigError(Exception):
    """Raised when train_params is not a dict, DefaultTrainingConfig, or None."""

    pass
