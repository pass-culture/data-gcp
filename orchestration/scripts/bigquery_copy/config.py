from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "migrated_dags.yaml"
CONFIG_BASE_DIR = DEFAULT_CONFIG_PATH.parent
COPY_JOB_POLL_INTERVAL_SECONDS = 10
ESTIMATED_TRANSFER_USD_PER_GIB = 0.02
DEFAULT_JOB_LOG_DATASET_ID = "copy_job_monitoring"
DEFAULT_JOB_LOG_TABLE_ID = "copy_jobs"


@dataclass
class EnvironmentConfig:
    """BigQuery projects and dataset suffixes."""

    source_project: str
    source_suffix: str
    destination_project: str
    destination_suffix: str

    def source_dataset_id(self, base_dataset: str) -> str:
        """Return the suffixed source dataset id for ``base_dataset``."""

        return f"{base_dataset}_{self.source_suffix}"

    def destination_dataset_id(self, base_dataset: str) -> str:
        """Return the suffixed destination dataset id for ``base_dataset``."""

        return f"{base_dataset}_{self.destination_suffix}"


@dataclass
class DagCopyConfig:
    """Copy configuration for a migrated DAG."""

    source_dataset: str
    tables_to_copy: tuple[str, ...]


@dataclass
class JobLogConfig:
    """BigQuery table used to persist copy-job metadata."""

    dataset_id: str = DEFAULT_JOB_LOG_DATASET_ID
    table_id: str = DEFAULT_JOB_LOG_TABLE_ID


@dataclass
class TableCopyEstimate:
    """Estimated cost for copying a single table."""

    table_name: str
    row_count: int
    estimated_bytes: int
    estimated_price_usd: float


@dataclass
class CopyPlan:
    """Full estimate for a DAG copy, with a per-table breakdown."""

    tables: list[TableCopyEstimate]
    source_location: str
    destination_location: str

    @property
    def total_rows(self) -> int:
        return sum(table.row_count for table in self.tables)

    @property
    def total_bytes(self) -> int:
        return sum(table.estimated_bytes for table in self.tables)

    @property
    def total_price_usd(self) -> float:
        return sum(table.estimated_price_usd for table in self.tables)


def resolve_config_path(config_path: Path = DEFAULT_CONFIG_PATH) -> Path:
    """Resolve ``config_path`` and ensure it stays within ``CONFIG_BASE_DIR``."""

    resolved = Path(config_path).resolve()
    if not resolved.is_relative_to(CONFIG_BASE_DIR):
        raise ValueError(
            f"Configuration path '{config_path}' is outside the allowed "
            f"directory '{CONFIG_BASE_DIR}'."
        )

    return resolved


def load_yaml_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load the migration config from ``migrated_dags.yaml``."""

    safe_path = resolve_config_path(config_path)
    try:
        with safe_path.open() as stream:
            config = yaml.safe_load(stream) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Configuration file not found: {safe_path}") from exc

    return config


def load_environment_config(config: dict[str, Any], env: str) -> EnvironmentConfig:
    """Load the environment block for ``env``."""

    environments = config.get("environments", {})
    if env not in environments:
        raise KeyError(f"Environment '{env}' not found in configuration.")

    env_config = environments[env]

    return EnvironmentConfig(
        source_project=str(env_config["source_project"]),
        source_suffix=str(env_config["source_suffix"]),
        destination_project=str(env_config["destination_project"]),
        destination_suffix=str(env_config["destination_suffix"]),
    )


def load_dag_copy_config(config: dict[str, Any], dag_name: str) -> DagCopyConfig:
    """Load the copy configuration for a migrated DAG."""

    dags_in_paris = config.get("dags_in_paris", {})
    if dag_name not in dags_in_paris:
        raise KeyError(f"DAG '{dag_name}' not found in configuration.")

    dag_config = dags_in_paris[dag_name] or {}
    source_dataset = dag_config.get("source_dataset")
    if not source_dataset:
        raise ValueError(
            f"Missing 'source_dataset' for DAG '{dag_name}' in configuration."
        )
    raw_tables = dag_config.get("tables_to_copy") or []
    if not isinstance(raw_tables, list):
        raise ValueError("'tables_to_copy' must be a list.")

    return DagCopyConfig(
        source_dataset=str(source_dataset),
        tables_to_copy=tuple(raw_tables),
    )
