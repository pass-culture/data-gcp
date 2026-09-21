"""Local checkpointing for the two-pass discovery+hydration extraction pattern.

Used by `cli/extract_from_wikidata.py::extract` to resume a two-pass target
(QueryConfig.hydration_batch_size) after a mid-run failure, per
wikidata_extraction_pipeline_specification.md's "Checkpointing" section: Pass 1's
result and each hydrated Pass 2 batch are persisted here as `extract` goes, so an
Airflow-level retry of the same task reuses whatever it already completed instead
of redoing it from scratch.

This relies on how the extraction DAG runs `extract`: the repo is cloned onto the
extraction VM once per DAG run (before any `extract_{target}` task), and an
Airflow-level retry of `extract_{target}` re-SSHes into that same still-running VM
without re-cloning — so `CHECKPOINT_ROOT_DIR`, a path relative to the repo
checkout, survives exactly the retries it needs to and no more (a fresh VM next
month starts with no checkpoint directory at all).
"""

import json
import os
import shutil

import pandas as pd

CHECKPOINT_ROOT_DIR = ".wikidata_checkpoint"


def checkpoint_dir_for(query_name: str) -> str:
    return os.path.join(CHECKPOINT_ROOT_DIR, query_name)


def _discovery_path(checkpoint_dir: str) -> str:
    return os.path.join(checkpoint_dir, "discovery.parquet")


def load_discovery_checkpoint(checkpoint_dir: str) -> pd.DataFrame | None:
    path = _discovery_path(checkpoint_dir)
    return pd.read_parquet(path) if os.path.exists(path) else None


def save_discovery_checkpoint(checkpoint_dir: str, discovery_df: pd.DataFrame) -> None:
    os.makedirs(checkpoint_dir, exist_ok=True)
    discovery_df.to_parquet(_discovery_path(checkpoint_dir), index=False)


def _batch_path(checkpoint_dir: str, batch_index: int) -> str:
    return os.path.join(checkpoint_dir, "batches", f"{batch_index}.parquet")


def load_batch_checkpoint(checkpoint_dir: str, batch_index: int) -> pd.DataFrame | None:
    path = _batch_path(checkpoint_dir, batch_index)
    return pd.read_parquet(path) if os.path.exists(path) else None


def save_batch_checkpoint(
    checkpoint_dir: str, batch_index: int, batch_df: pd.DataFrame
) -> None:
    path = _batch_path(checkpoint_dir, batch_index)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    batch_df.to_parquet(path, index=False)


def _processed_batches_log_path(checkpoint_dir: str) -> str:
    return os.path.join(checkpoint_dir, "processed_batches.log")


def load_processed_batches(checkpoint_dir: str) -> set[int]:
    path = _processed_batches_log_path(checkpoint_dir)
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {int(line) for line in f if line.strip()}


def mark_batch_processed(checkpoint_dir: str, batch_index: int) -> None:
    # Append, not rewrite: each batch is marked done once, right after it's
    # fetched — a crash mid-run leaves every prior line intact.
    with open(_processed_batches_log_path(checkpoint_dir), "a") as f:
        f.write(f"{batch_index}\n")


def _dropped_ids_path(checkpoint_dir: str) -> str:
    return os.path.join(checkpoint_dir, "dropped_ids.json")


def load_dropped_ids(checkpoint_dir: str) -> list[str]:
    path = _dropped_ids_path(checkpoint_dir)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def save_dropped_ids(checkpoint_dir: str, dropped_ids: list[str]) -> None:
    with open(_dropped_ids_path(checkpoint_dir), "w") as f:
        json.dump(dropped_ids, f)


def clear_checkpoint(checkpoint_dir: str) -> None:
    """Remove a target's whole checkpoint directory. Call only on a successful
    `extract` run — a failed/raised attempt should leave it in place for the next
    Airflow-level retry to resume from."""
    if os.path.isdir(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)
