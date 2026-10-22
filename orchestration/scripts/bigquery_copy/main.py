from __future__ import annotations

import argparse
import logging
from pathlib import Path

from google.cloud import bigquery

from .config import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_JOB_LOG_DATASET_ID,
    DEFAULT_JOB_LOG_TABLE_ID,
    JobLogConfig,
    load_dag_copy_config,
    load_environment_config,
    load_yaml_config,
)
from .copy import copy_dag_tables
from .monitoring import show_recent_copy_jobs


def main() -> None:
    """Copy data from one BigQuery dataset to another based on the migrated DAG config."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )

    parser = argparse.ArgumentParser(prog="bigquery_copy")
    parser.add_argument(
        "--dag-name",
        help="Name of the migrated DAG to copy. Only required for copy mode.",
    )
    parser.add_argument(
        "--env",
        help="Environment to run the copy in (tst, stg, prd). Only required for copy mode.",
    )
    parser.add_argument(
        "--config-path",
        default=DEFAULT_CONFIG_PATH,
        help="Path to migrated_dags.yaml.",
    )
    parser.add_argument(
        "--list-copy-jobs",
        action="store_true",
        help="List recent BigQuery COPY jobs for the target project instead of copying tables.",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip the confirmation prompt and start the copy immediately.",
    )
    args = parser.parse_args()

    if args.list_copy_jobs:
        if not args.env:
            raise SystemExit("--env is required when using --list-copy-jobs.")
        client = bigquery.Client(
            project=load_environment_config(
                load_yaml_config(Path(args.config_path)), args.env
            ).destination_project
        )
        show_recent_copy_jobs(client)
        return

    if not args.dag_name or not args.env:
        raise SystemExit("--dag-name and --env are required for copy mode.")

    config = load_yaml_config(Path(args.config_path))
    env_config = load_environment_config(config, args.env)
    dag_copy_config = load_dag_copy_config(config, args.dag_name)
    copy_dag_tables(
        env_config,
        args.dag_name,
        dag_copy_config,
        JobLogConfig(
            dataset_id=DEFAULT_JOB_LOG_DATASET_ID,
            table_id=DEFAULT_JOB_LOG_TABLE_ID,
            location=None,
        ),
        assume_yes=args.yes,
    )


if __name__ == "__main__":
    main()
