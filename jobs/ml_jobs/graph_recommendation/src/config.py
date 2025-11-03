import json
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

from loguru import logger

from src.constants import ARTIST_ID_COLUMN, GTL_ID_COLUMN, ID_COLUMN


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
            raise InvalidConfigError("Invalid JSON string") from e
        self.update_from_dict(updates)

    def update_from_dict(self, config_dict: dict):
        """Update fields from a dictionary, logging errors instead of raising."""
        if not isinstance(config_dict, dict):
            logger.error("config_dict must be a dictionary")
            raise InvalidConfigError("Invalid config_dict")
        for k, v in config_dict.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                logger.warning(f"Ignored unknown config field: {k}")

    def to_dict(self) -> dict[str, Any]:
        """Return config as a dictionary."""
        return asdict(self)


class InvalidConfigError(Exception):
    """Raised when train_params is not a dict, DefaultTrainingConfig, or None."""

    pass


@dataclass
class TrainingConfig(BaseConfig):
    embedding_dim: int = 128
    metapath_repetitions: int = 4
    context_size: int = 10
    walks_per_node: int = 5
    num_negative_samples: int = 5
    num_epochs: int = 8
    early_stop: bool = True
    num_workers: int = 8 if sys.platform == "linux" else 0
    batch_size: int = 256
    learning_rate: float = 0.01
    metapath: list[tuple[str, str, str]] = field(
        default_factory=(
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
    )


@dataclass
class EvaluationConfig(BaseConfig):
    metadata_columns: list[str] = field(
        default_factory=lambda: [ID_COLUMN, GTL_ID_COLUMN, ARTIST_ID_COLUMN]
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
