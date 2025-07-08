"""
DBT execution functions using dbt-core Python API.
Simple implementation that mirrors the original bash scripts functionality.
"""

import os
import shutil
import tempfile
import logging
from pathlib import Path
from typing import List, Optional

from dbt.cli.main import dbtRunner, dbtRunnerResult

from common.config import (
    ENV_SHORT_NAME,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
    EXCLUDED_TAGS,
)

logger = logging.getLogger(__name__)


def create_tmp_folder(base_path: str) -> str:
    """Create a temporary directory for dbt target files."""
    temp_dir = tempfile.mkdtemp(prefix="dbt_target_", dir="/tmp")
    logger.debug(f"Created temporary directory: {temp_dir}")

    # Copy existing target files to temp directory
    if os.path.exists(base_path):
        for item in os.listdir(base_path):
            s = os.path.join(base_path, item)
            d = os.path.join(temp_dir, item)
            if os.path.isdir(s):
                shutil.copytree(s, d)
            else:
                shutil.copy2(s, d)

    return temp_dir


def cleanup_temp_dir(temp_dir: str) -> None:
    """Clean up temporary directory."""
    if os.path.exists(temp_dir) and temp_dir.startswith("/tmp/dbt_target_"):
        shutil.rmtree(temp_dir)
        logger.debug(f"Removed temporary directory: {temp_dir}")


def run_dbt_command(command: list, **context) -> None:
    """Execute a dbt command using dbtRunner."""
    # Get parameters from context
    params = context.get("params", {})
    target = params.get("target", ENV_SHORT_NAME)
    global_cli_flags = params.get("GLOBAL_CLI_FLAGS", "--no-write-json")

    # Create temporary target directory
    temp_target_dir = create_tmp_folder(PATH_TO_DBT_TARGET)

    try:
        # Initialize dbt runner
        dbt = dbtRunner()

        # Build the CLI args
        cli_args = command.copy()
        cli_args.extend(["--target", target])
        cli_args.extend(["--target-path", temp_target_dir])
        cli_args.extend(["--vars", f"{{ENV_SHORT_NAME: {ENV_SHORT_NAME}}}"])
        cli_args.extend(["--project-dir", PATH_TO_DBT_PROJECT])

        # Add global CLI flags if they exist
        if global_cli_flags:
            cli_args.extend(global_cli_flags.split())

        logger.info(f"Executing dbt command: {' '.join(cli_args)}")

        # Execute dbt command
        res: dbtRunnerResult = dbt.invoke(cli_args)

        # Check for errors
        if not res.success:
            error_msg = f"dbt command failed with exit code {res.exception}."
            logger.error(error_msg)
            raise Exception(error_msg)

        logger.info("DBT run completed successfully.")

    finally:
        # Clean up temporary directory
        cleanup_temp_dir(temp_target_dir)


def compile_dbt(**context):
    """Compile dbt project."""
    run_dbt_command(["compile"], **context)


def clean_dbt(**context):
    """Clean dbt artifacts."""
    # Clean temporary folders
    temp_folders = Path("/tmp").glob("dbt_target_*")
    for folder in temp_folders:
        try:
            shutil.rmtree(folder)
        except Exception as e:
            logger.warning(f"Could not remove {folder}: {e}")

    # Run dbt clean
    run_dbt_command(["clean"], **context)


def run_dbt_model(model_name: str, exclude_tags: Optional[List[str]] = None, **context):
    """Run a specific dbt model."""
    params = context.get("params", {})
    full_refresh = params.get("full_refresh", False)

    command = ["run", "--select", model_name]

    if full_refresh:
        command.append("--full-refresh")

    if exclude_tags:
        for tag in exclude_tags:
            command.extend(["--exclude", f"tag:{tag}"])

    run_dbt_command(command, **context)


def run_dbt_test(model_name: str, **context):
    """Run tests for a specific dbt model."""
    command = ["test", "--select", f"{model_name},tag:critical"]
    run_dbt_command(command, **context)


def run_dbt_snapshot(snapshot_name: str, **context):
    """Run a specific dbt snapshot."""
    command = ["snapshot", "--select", snapshot_name]
    run_dbt_command(command, **context)
