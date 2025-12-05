import json
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field

from loguru import logger

from src.constants import ARTIST_ID_COLUMN, GTL_ID_COLUMN, ID_COLUMN, SERIES_ID_COLUMN
from src.utils.metadata_metrics import get_gtl_retrieval_score


class BaseConfig:
    """Base class for configuration objects with JSON/dict update support."""

    def parse_and_update_config(self, json_str: str) -> "BaseConfig":
        """Parse JSON string and update configuration fields.

        Parses a JSON string containing configuration updates and applies them
        to the current configuration object. Unknown fields are logged as warnings
        but do not cause errors.

        Args:
            json_str: JSON string containing configuration field updates.
                     Must be valid JSON that deserializes to a dictionary.

        Returns:
            Self: The updated configuration object for method chaining.

        Raises:
            InvalidConfigError: If json_str is not valid JSON or if the parsed
                               result is not a dictionary.

        Example:
            >>> config = TrainingConfig()
            >>> config.parse_and_update_config('{"embedding_dim": 64, "num_epochs": 5}')
            >>> config.embedding_dim
            64
        """

        try:
            updates = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON for config: {e}")
            raise InvalidConfigError("Invalid JSON string") from e
        return self._update_from_dict(updates)

    def _update_from_dict(self, config_dict: dict) -> "BaseConfig":
        """Update configuration fields from a dictionary.

        Updates the current configuration object with values from the provided
        dictionary. Only updates fields that already exist as attributes on the
        configuration object. Unknown fields are logged as warnings but ignored.

        Args:
            config_dict: Dictionary containing configuration field updates.
                        Keys should match existing attribute names on the config object.

        Returns:
            Self: The updated configuration object for method chaining.

        Raises:
            InvalidConfigError: If config_dict is not a dictionary.

        Example:
            >>> config = TrainingConfig()
            >>> config._update_from_dict({"embedding_dim": 64, "unkn_field": "value"})
            # Sets embedding_dim to 64, logs warning about unkn_field
        """

        if not isinstance(config_dict, dict):
            logger.error("config_dict must be a dictionary")
            raise InvalidConfigError("Invalid config_dict")
        for k, v in config_dict.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                logger.warning(f"Ignored unknown config field: {k}")
        return self

    def to_dict(self) -> dict:
        """Return config as a dictionary."""
        return asdict(self)


class InvalidConfigError(Exception):
    """Raised when train_params is not a dict, DefaultTrainingConfig, or None."""

    pass


@dataclass
class TrainingConfig(BaseConfig):
    embedding_dim: int = 64
    walk_length: int = 20
    context_size: int = 10
    walks_per_node: int = 5
    num_negative_samples: int = 5
    num_epochs: int = 8
    num_workers: int = 8 if sys.platform == "linux" else 0
    batch_size: int = 256
    learning_rate: float = 0.03
    early_stop: bool = True
    early_stopping_delta: float = 0.001
    metapaths: list[list[tuple[str, str, str]]] = field(
        default_factory=lambda: (
            [
                [
                    ("book", "is_artist_id", "artist_id"),
                    ("artist_id", "artist_id_of", "book"),
                ],
                [
                    ("book", "is_gtl_label_level_4", "gtl_label_level_4"),
                    ("gtl_label_level_4", "gtl_label_level_4_of", "book"),
                ],
                [
                    ("book", "is_gtl_label_level_3", "gtl_label_level_3"),
                    ("gtl_label_level_3", "gtl_label_level_3_of", "book"),
                ],
                [
                    ("book", "is_series_id", "series_id"),
                    ("series_id", "series_id_of", "book"),
                ],
                [
                    ("book", "is_gtl_label_level_2", "gtl_label_level_2"),
                    ("gtl_label_level_2", "gtl_label_level_2_of", "book"),
                ],
                [
                    ("book", "is_gtl_label_level_1", "gtl_label_level_1"),
                    ("gtl_label_level_1", "gtl_label_level_1_of", "book"),
                ],
            ]
        )
    )


@dataclass
class EvaluationConfig(BaseConfig):
    node_id_column: str = ID_COLUMN
    metadatas_with_categorical_scoring: list[str] = field(
        default_factory=lambda: [ARTIST_ID_COLUMN, SERIES_ID_COLUMN]
    )
    metadatas_with_custom_scoring: dict[str, Callable[[str, str], float]] = field(
        default_factory=lambda: {GTL_ID_COLUMN: get_gtl_retrieval_score}
    )  # Could also be {GTL_ID_COLUMN: get_gtl_walk_score}
    n_samples: int = 1_000
    n_retrieved: int = 10_000
    k_values: list[int] = field(default_factory=lambda: [10, 20, 50, 100])
    rebuild_index: bool = True

    @property
    def metadata_columns(self) -> list[str]:
        return [
            *self.metadatas_with_custom_scoring.keys(),
            *self.metadatas_with_categorical_scoring,
        ]

    def to_dict(self) -> dict:
        """Return config as a dictionary with function names instead of objects."""
        result = asdict(self)
        # Replace function objects with their names
        result["metadatas_with_custom_scoring"] = {
            k: v.__name__ for k, v in self.metadatas_with_custom_scoring.items()
        }
        return result
