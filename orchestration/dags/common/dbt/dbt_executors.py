"""
DBT execution functions using dbt-core Python API.
Simple implementation that mirrors the original bash scripts functionality.
"""

import os
import shutil
from copy import copy
import tempfile
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from dbt.cli.main import dbtRunner, dbtRunnerResult
from airflow.exceptions import AirflowSkipException

from common.config import (
    ENV_SHORT_NAME,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
    EXCLUDED_TAGS,
)


def should_skip_scheduled_node(node_tags: List[str], ds: str) -> Optional[str]:
    """
    Check if a node should be skipped based on its schedule tags and the execution date.

    Args:
        node_tags: List of tags for the node
        ds: Airflow execution date string (YYYY-MM-DD)

    Returns:
        Schedule type ('weekly' or 'monthly') if should skip, None otherwise
    """
    if not node_tags or not ds:
        return None

    try:
        execution_date = datetime.strptime(ds, "%Y-%m-%d")

        # Check for weekly tag - run only on first day of week (Monday)
        if "weekly" in node_tags:
            if execution_date.weekday() != 0:  # 0 is Monday
                return "weekly"

        # Check for monthly tag - run only on first day of month
        if "monthly" in node_tags:
            if execution_date.day != 1:
                return "monthly"

    except ValueError:
        logging.warning(f"Invalid date format for ds: {ds}")

    return None


def create_tmp_folder(base_path: str) -> str:
    """Create a temporary directory for dbt target files."""
    temp_dir = tempfile.mkdtemp(prefix="dbt_target_", dir="/tmp")
    logging.debug(f"Created temporary directory: {temp_dir}")

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
        logging.debug(f"Removed temporary directory: {temp_dir}")


def run_dbt_command(command: list, use_tmp_artifacts: bool = True, **context) -> None:
    """Execute a dbt command using dbtRunner."""
    # Get parameters from context
    params = context.get("params", {})
    target = params.get("target", ENV_SHORT_NAME)
    global_cli_flags = params.get("GLOBAL_CLI_FLAGS", None)

    # Create temporary target directory
    if use_tmp_artifacts:
        temp_target_dir = create_tmp_folder(PATH_TO_DBT_TARGET)
    else:
        temp_target_dir = PATH_TO_DBT_TARGET

    try:
        # Initialize dbt runner
        dbt = dbtRunner()

        # Build the CLI args
        cli_args = command.copy()
        cli_args.extend(["--target", target])
        cli_args.extend(["--target-path", temp_target_dir])
        cli_args.extend(["--vars", f"{{ENV_SHORT_NAME: {ENV_SHORT_NAME}}}"])
        cli_args.extend(["--project-dir", PATH_TO_DBT_PROJECT])
        cli_args.extend(["--profiles-dir", PATH_TO_DBT_PROJECT])

        # Add global CLI flags if they exist

        if global_cli_flags is not None or global_cli_flags != "":
            tmp_cli_flags = copy(global_cli_flags)
            if isinstance(tmp_cli_flags, str):
                # Ensure the string is split into a list
                tmp_cli_flags = tmp_cli_flags.strip()
            else:
                raise TypeError(
                    f"Invalid type for GLOBAL_CLI_FLAGS: {type(global_cli_flags)}. Expected str"
                )
            g_cli_flags_list = tmp_cli_flags.split()
            assert all(
                [flag.startswith("--") for flag in g_cli_flags_list]
            ), "All flags in GLOBAL_CLI_FLAGS must start with '--'."

            if "compile" in command:
                g_cli_flags_list = tmp_cli_flags.split()
                # Remove --no-write-json if it exists
                if "--no-write-json" in g_cli_flags_list:
                    g_cli_flags_list.remove("--no-write-json")

            tmp_cli_flags = " ".join(g_cli_flags_list)
            cli_args.extend(tmp_cli_flags.split())

        logging.info(f"Executing dbt command: {' '.join(cli_args)}")

        # Execute dbt command
        res: dbtRunnerResult = dbt.invoke(cli_args)

        # Check for errors
        if not res.success:
            error_msg = f"dbt command failed with exit code {res.exception}."
            logging.error(error_msg)
            raise Exception(error_msg)

        logging.info("DBT run completed successfully.")

    finally:
        # Clean up temporary directory
        if use_tmp_artifacts:
            cleanup_temp_dir(temp_target_dir)


def run_dbt_with_selector(selector: str, **context):
    """
    Run dbt with a specific selector.
    Args:
        selector: dbt selector (e.g., "package:elementary")
        **context: Airflow context
    """
    command = ["run", "--select", selector]
    run_dbt_command(command, **context)


def compile_dbt(use_tmp_artifacts: bool = False, **context):
    """
    Compile dbt project.

    Args:
        use_tmp_artifacts: If True, use temporary directory. If False, compile directly to target.
        **context: Airflow context
    """

    run_dbt_command(["compile"], use_tmp_artifacts=use_tmp_artifacts, **context)


def compile_dbt_with_selector(
    selector: str, use_tmp_artifacts: bool = False, **context
):
    """
    Compile dbt project.

    Args:
        selector: dbt selector (e.g., "package:data_gcp_dbt")
        use_tmp_artifacts: If True, use temporary directory. If False, compile directly to target.
        **context: Airflow context
    """
    command = ["compile", "--select", selector]
    run_dbt_command(command, use_tmp_artifacts=use_tmp_artifacts, **context)


def clean_dbt(**context):
    """Clean dbt artifacts."""
    # Clean temporary folders
    temp_folders = Path("/tmp").glob("dbt_target_*")
    for folder in temp_folders:
        try:
            shutil.rmtree(folder)
        except Exception as e:
            logging.warning(f"Could not remove {folder}: {e}")


def run_dbt_model(
    model_name: str,
    exclude_tags: Optional[List[str]] = None,
    node_tags: Optional[List[str]] = None,
    **context,
):
    """Run a specific dbt model."""
    # Check if we should skip based on schedule
    ds = context.get("ds")
    skip_schedule = should_skip_scheduled_node(node_tags or [], ds)
    if skip_schedule:
        logging.info(f"Skipping node scheduled {skip_schedule}")
        raise AirflowSkipException(f"Skipping node scheduled {skip_schedule}")

    params = context.get("params", {})
    full_refresh = params.get("full_refresh", False)

    command = ["run", "--select", model_name]

    if full_refresh:
        command.append("--full-refresh")

    if exclude_tags:
        for tag in exclude_tags:
            command.extend(["--exclude", f"tag:{tag}"])

    run_dbt_command(command, **context)


def run_dbt_test(model_name: str, node_tags: Optional[List[str]] = None, **context):
    """Run tests for a specific dbt model."""
    # Check if we should skip based on schedule
    ds = context.get("ds")
    skip_schedule = should_skip_scheduled_node(node_tags or [], ds)
    if skip_schedule:
        logging.info(f"Skipping node scheduled {skip_schedule}")
        raise AirflowSkipException(f"Skipping node scheduled {skip_schedule}")

    command = ["test", "--select", f"{model_name},tag:critical"]
    run_dbt_command(command, **context)


def run_dbt_quality_tests(
    select: Optional[str] = None, exclude: Optional[str] = None, **context
):
    """
    Run dbt tests with custom select and exclude patterns.

    Args:
        select: dbt selector pattern (e.g., "tag:weekly")
        exclude: dbt exclusion pattern (e.g., "audit tag:export")
        **context: Airflow context
    """
    command = ["test"]

    if select:
        command.extend(["--select", select])

    if exclude:
        # Split multiple exclusions and add each
        for exclusion in exclude.split():
            command.extend(["--exclude", exclusion])

    run_dbt_command(command, **context)


def run_dbt_snapshot(
    snapshot_name: str, node_tags: Optional[List[str]] = None, **context
):
    """Run a specific dbt snapshot."""
    # Check if we should skip based on schedule
    ds = context.get("ds")
    skip_schedule = should_skip_scheduled_node(node_tags or [], ds)
    if skip_schedule:
        logging.info(f"Skipping node scheduled {skip_schedule}")
        raise AirflowSkipException(f"Skipping node scheduled {skip_schedule}")

    command = ["snapshot", "--select", snapshot_name]
    run_dbt_command(command, **context)


def run_dbt_operation(operation: str, args: Optional[str] = None, **context):
    """
    Run a dbt operation (macro).

    Args:
        operation: Name of the dbt operation/macro to run
        args: JSON string of arguments to pass to the operation
        **context: Airflow context
    """
    command = ["run-operation", operation]

    if args:
        command.extend(["--args", args])

    run_dbt_command(command, **context)
