from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "migrated_dags.yaml"
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


@dataclass
class DagCopyConfig:
    """Copy configuration for a migrated DAG."""

    source_dataset: str
    tables_to_copy: tuple[str, ...]


@dataclass
class JobLogConfig:
    """BigQuery table used to persist copy-job metadata."""

    dataset_id: str
    table_id: str = DEFAULT_JOB_LOG_TABLE_ID
    location: str | None = None


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


def load_yaml_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load the migration config from ``migrated_dags.yaml``."""

    try:
        with config_path.open() as stream:
            config = yaml.safe_load(stream) or {}
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Configuration file not found: {config_path}") from exc

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
