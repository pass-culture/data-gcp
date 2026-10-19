from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)
DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("migrated_dags.yaml")
COPY_JOB_POLL_INTERVAL_SECONDS = 10
ESTIMATED_TRANSFER_USD_PER_GIB = 0.02
DEFAULT_JOB_LOG_TABLE_ID = "copy_jobs"

JOB_LOG_TABLE_SCHEMA = [
    bigquery.SchemaField("logged_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dag_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("copy_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("job_id", "STRING"),
    bigquery.SchemaField("job_project", "STRING"),
    bigquery.SchemaField("job_location", "STRING"),
    bigquery.SchemaField("job_state", "STRING"),
    bigquery.SchemaField("job_type", "STRING"),
    bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_location", "STRING"),
    bigquery.SchemaField("destination_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("destination_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("destination_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("destination_location", "STRING"),
    bigquery.SchemaField("source_row_count", "INT64"),
    bigquery.SchemaField("destination_row_count", "INT64"),
    bigquery.SchemaField("estimated_bytes", "INT64"),
    bigquery.SchemaField("estimated_price_usd", "FLOAT64"),
    bigquery.SchemaField("copy_started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("copy_ended_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("duration_seconds", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("error_message", "STRING"),
]


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
    source_dataset = str(dag_config.get("source_dataset"))
    raw_tables = dag_config.get("tables_to_copy") or []
    if not isinstance(raw_tables, list):
        raise ValueError("'tables_to_copy' must be a list.")

    return DagCopyConfig(
        source_dataset=source_dataset,
        tables_to_copy=tuple(raw_tables),
    )


def _to_rfc3339(value: datetime) -> str:
    """Convert a datetime to RFC3339 for BigQuery JSON inserts."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).isoformat()


def ensure_job_log_table(
    client: bigquery.Client,
    project_id: str,
    config: JobLogConfig,
) -> bigquery.TableReference:
    """Create the logging dataset/table if they do not exist yet."""

    dataset_ref = bigquery.DatasetReference(project_id, config.dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, config.table_id)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        if not config.location:
            raise ValueError(
                "A job log location is required to create the logging dataset."
            )

        dataset = bigquery.Dataset(f"{project_id}.{config.dataset_id}")  # type: ignore[arg-type]
        dataset.location = config.location
        client.create_dataset(dataset)
        LOGGER.info(
            "Created BigQuery logging dataset %s in location %s",
            dataset_ref.path,
            config.location,
        )

    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(  # type: ignore[arg-type]
            f"{project_id}.{config.dataset_id}.{config.table_id}",
            schema=JOB_LOG_TABLE_SCHEMA,
        )
        client.create_table(table)
        LOGGER.info("Created BigQuery logging table %s", table_ref.path)

    return table_ref


def write_copy_job_log(
    client: bigquery.Client,
    table_ref: bigquery.TableReference,
    row: dict[str, Any],
) -> None:
    """Append one copy-job monitoring row to BigQuery."""

    row_id = f"{row.get('job_id') or 'no-job'}-{row['source_table']}-{row['copy_started_at']}"
    errors = client.insert_rows_json(table_ref, [row], row_ids=[row_id])
    if errors:
        raise RuntimeError(
            f"Failed to insert copy-job log row into {table_ref.path}: {errors}"
        )


def copy_table(
    client: bigquery.Client,
    environment: EnvironmentConfig,
    dag_name: str,
    env: str,
    source_dataset: str,
    table_name: str,
    job_log_table: bigquery.TableReference | None = None,
) -> None:
    """Copy one table from the source project to the destination project."""

    started_at = time.monotonic()
    started_at_utc = datetime.now(timezone.utc)
    source_dataset_id = f"{source_dataset}_{environment.source_suffix}"
    destination_dataset_id = f"{source_dataset}_{environment.destination_suffix}"

    source_ref = bigquery.TableReference.from_string(
        f"{environment.source_project}.{source_dataset_id}.{table_name}"
    )
    destination_ref = bigquery.TableReference.from_string(
        f"{environment.destination_project}.{destination_dataset_id}.{table_name}"
    )

    copy_job: bigquery.job.base._AsyncJob | None = None
    source_location: str | None = None
    destination_location: str | None = None
    source_row_count: int | None = None
    destination_row_count: int | None = None
    estimated_bytes = 0
    estimated_price_usd = 0.0
    copy_status = "FAILED"
    error_message: str | None = None

    try:
        source_dataset_obj = client.get_dataset(
            f"{environment.source_project}.{source_dataset_id}"
        )
        destination_dataset_obj = client.get_dataset(
            f"{environment.destination_project}.{destination_dataset_id}"
        )
        source_location = source_dataset_obj.location
        destination_location = destination_dataset_obj.location
        assert source_location is not None
        assert destination_location is not None

        source_table = client.get_table(source_ref)
        source_row_count = source_table.num_rows
        estimated_bytes = source_table.num_bytes or 0
        estimated_price_usd = estimate_copy_price_usd(estimated_bytes)

        copy_job = client.copy_table(
            source_ref,
            destination_ref,
            location=source_location,
            job_config=bigquery.CopyJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        LOGGER.info(
            "Submitted BigQuery copy job for %s.%s: job_id=%s project=%s location=%s source=%s destination=%s",
            source_dataset_id,
            table_name,
            copy_job.job_id,
            environment.destination_project,
            source_location,
            source_ref,
            destination_ref,
        )
        LOGGER.info(
            "Manual follow-up: bq show --job --project_id=%s --location=%s %s",
            environment.destination_project,
            source_location,
            copy_job.job_id,
        )
        assert copy_job is not None
        wait_for_job(copy_job)

        destination_table = client.get_table(destination_ref)
        destination_row_count = destination_table.num_rows
        check_copy_succeeded(
            client,
            source_ref,
            destination_ref,
            source_table,
            destination_table,
            source_location,
            destination_location,
        )
        copy_status = "SUCCESS"
        LOGGER.info(
            "All copy checks OK for %s.%s (rows copied=%s)",
            source_ref.dataset_id,
            table_name,
            destination_table.num_rows,
        )
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        ended_at_utc = datetime.now(timezone.utc)
        elapsed_seconds = time.monotonic() - started_at
        LOGGER.info(
            "Copy attempt for %s.%s took %.2fs",
            source_ref.dataset_id,
            table_name,
            elapsed_seconds,
        )
        if job_log_table is not None:
            job_project = getattr(copy_job, "project", environment.destination_project)
            job_location = getattr(copy_job, "location", source_location)
            job_state = getattr(copy_job, "state", None)

            try:
                write_copy_job_log(
                    client,
                    job_log_table,
                    {
                        "logged_at": _to_rfc3339(ended_at_utc),
                        "dag_name": dag_name,
                        "environment": env,
                        "copy_status": copy_status,
                        "job_id": getattr(copy_job, "job_id", None),
                        "job_project": job_project,
                        "job_location": job_location,
                        "job_state": job_state,
                        "job_type": "COPY",
                        "source_project": source_ref.project,
                        "source_dataset": source_ref.dataset_id,
                        "source_table": source_ref.table_id,
                        "source_location": source_location,
                        "destination_project": destination_ref.project,
                        "destination_dataset": destination_ref.dataset_id,
                        "destination_table": destination_ref.table_id,
                        "destination_location": destination_location,
                        "source_row_count": source_row_count,
                        "destination_row_count": destination_row_count,
                        "estimated_bytes": estimated_bytes,
                        "estimated_price_usd": estimated_price_usd,
                        "copy_started_at": _to_rfc3339(started_at_utc),
                        "copy_ended_at": _to_rfc3339(ended_at_utc),
                        "duration_seconds": elapsed_seconds,
                        "error_message": error_message,
                    },
                )
                LOGGER.info(
                    "Persisted copy log row to %s",
                    job_log_table.path,
                )
            except Exception:
                LOGGER.exception(
                    "Failed to persist copy log for %s.%s",
                    source_ref.dataset_id,
                    table_name,
                )


def wait_for_job(job: bigquery.job.base._AsyncJob) -> None:
    """Poll a BigQuery job until completion and log its state along the way."""

    LOGGER.info(
        "Started BigQuery copy job %s (state=%s, location=%s)",
        job.job_id,
        job.state,
        getattr(job, "location", "unknown"),
    )
    while not job.done():
        time.sleep(COPY_JOB_POLL_INTERVAL_SECONDS)
        job.reload()
        LOGGER.info("BigQuery copy job %s state=%s", job.job_id, job.state)

    job.result()
    LOGGER.info(
        "BigQuery copy job %s finished with state=%s; manual status check: gcloud bigquery jobs describe %s --project=%s --location=%s",
        job.job_id,
        job.state,
        job.job_id,
        job.project,
        getattr(job, "location", "unknown"),
    )


def check_copy_succeeded(
    client: bigquery.Client,
    source_ref: bigquery.TableReference,
    destination_ref: bigquery.TableReference,
    source_table: bigquery.Table,
    destination_table: bigquery.Table,
    source_location: str,
    destination_location: str,
) -> None:
    """
    Checks:
    - row count
    - schema names/types/modes
    - first and last partition for time-partitioned tables
    """

    if source_table.num_rows != destination_table.num_rows:
        raise ValueError(
            f"Row count mismatch after copy for {source_ref.path}: "
            f"{source_table.num_rows} != {destination_table.num_rows}"
        )

    LOGGER.info(
        "%s rows copied",
        destination_table.num_rows,
    )

    source_schema = {
        field.name: (field.field_type, field.mode) for field in source_table.schema
    }
    destination_schema = {
        field.name: (field.field_type, field.mode) for field in destination_table.schema
    }
    if source_schema != destination_schema:
        missing_columns = sorted(set(source_schema) - set(destination_schema))
        type_mismatches = sorted(
            column_name
            for column_name in set(source_schema) & set(destination_schema)
            if source_schema[column_name] != destination_schema[column_name]
        )
        raise ValueError(
            "Schema mismatch after copy for "
            f"{source_ref.path}: missing={missing_columns}, "
            f"type_mismatches={type_mismatches}"
        )

    source_is_partitioned = source_table.time_partitioning is not None

    if not source_is_partitioned:
        return

    source_first_partition, source_last_partition = get_partition_bounds(
        client,
        source_ref,
        source_location,
    )
    destination_first_partition, destination_last_partition = get_partition_bounds(
        client,
        destination_ref,
        destination_location,
    )

    if (
        source_first_partition != destination_first_partition
        or source_last_partition != destination_last_partition
    ):
        raise ValueError(
            "Partition bounds mismatch after copy for "
            f"{source_ref.path}: source=({source_first_partition}, {source_last_partition}), "
            f"destination=({destination_first_partition}, {destination_last_partition})"
        )


def get_partition_bounds(
    client: bigquery.Client,
    table_ref: bigquery.TableReference,
    location: str,
) -> tuple[str | None, str | None]:
    """Return the first and last partition for a table."""

    query = f"""
        SELECT
            MIN(partition_id) AS first_partition,
            MAX(partition_id) AS last_partition
        FROM `{table_ref.project}.{table_ref.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = @table_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "table_name",
                "STRING",
                table_ref.table_id,
            )
        ]
    )
    result = next(
        client.query(query, location=location, job_config=job_config).result(),
        None,
    )
    if result is None:
        return None, None

    return result.first_partition, result.last_partition


def estimate_copy_plan(
    client: bigquery.Client,
    environment: EnvironmentConfig,
    dag_copy_config: DagCopyConfig,
) -> tuple[int, int, str, str]:
    """Return total rows, total bytes and source/destination locations for a copy."""

    source_dataset_id = f"{dag_copy_config.source_dataset}_{environment.source_suffix}"
    destination_dataset_id = (
        f"{dag_copy_config.source_dataset}_{environment.destination_suffix}"
    )
    source_dataset_obj = client.get_dataset(
        f"{environment.source_project}.{source_dataset_id}"
    )
    destination_dataset_obj = client.get_dataset(
        f"{environment.destination_project}.{destination_dataset_id}"
    )

    total_rows = 0
    total_bytes = 0
    for table_name in dag_copy_config.tables_to_copy:
        source_ref = bigquery.TableReference.from_string(
            f"{environment.source_project}.{source_dataset_id}.{table_name}"
        )
        source_table = client.get_table(source_ref)
        total_rows += source_table.num_rows
        total_bytes += source_table.num_bytes or 0

    return (
        total_rows,
        total_bytes,
        source_dataset_obj.location,
        destination_dataset_obj.location,
    )


def estimate_copy_price_usd(
    total_bytes: int,
) -> float:
    """Estimate the transfer price in USD."""

    total_gib = total_bytes / (1024**3)

    return total_gib * ESTIMATED_TRANSFER_USD_PER_GIB


def confirm_copy_plan(
    total_rows: int,
    estimated_price_usd: float,
    source_location: str,
    destination_location: str,
) -> bool:
    """Ask the user to confirm the copy before running it."""

    pricing_note = (
        f"pricing transfer from {source_location} to {destination_location} "
        f"is {ESTIMATED_TRANSFER_USD_PER_GIB:.2f} $/GiB"
    )
    message = (
        f"{total_rows} rows will be copied and price ≈ {estimated_price_usd:.2f} USD "
        f"({pricing_note})."
    )
    LOGGER.info(message)
    answer = input(f"{message}\nProceed? [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def copy_dag_tables(
    environment: EnvironmentConfig,
    dag_name: str,
    env: str,
    dag_copy_config: DagCopyConfig,
    job_log_config: JobLogConfig | None = None,
) -> None:
    """Copy every table declared for a migrated DAG."""

    if not dag_copy_config.tables_to_copy:
        LOGGER.info("No tables to copy for this DAG.")
        return

    client = bigquery.Client(project=environment.destination_project)
    total_rows, total_bytes, source_location, destination_location = estimate_copy_plan(
        client,
        environment,
        dag_copy_config,
    )
    estimated_price_usd = estimate_copy_price_usd(
        total_bytes,
    )
    if not confirm_copy_plan(
        total_rows,
        estimated_price_usd,
        source_location,
        destination_location,
    ):
        LOGGER.info("Copy cancelled by user.")
        return

    job_log_table: bigquery.TableReference | None = None
    if job_log_config is not None:
        job_log_location = job_log_config.location or destination_location
        job_log_table = ensure_job_log_table(
            client,
            environment.destination_project,
            JobLogConfig(
                dataset_id=job_log_config.dataset_id,
                table_id=job_log_config.table_id,
                location=job_log_location,
            ),
        )

    for table_name in dag_copy_config.tables_to_copy:
        LOGGER.info(
            "Copying %s.%s (%s -> %s)",
            dag_copy_config.source_dataset,
            table_name,
            environment.source_project,
            environment.destination_project,
        )
        copy_table(
            client,
            environment,
            dag_name,
            env,
            dag_copy_config.source_dataset,
            table_name,
            job_log_table,
        )


def list_recent_copy_jobs(
    client: bigquery.Client,
) -> list[dict[str, Any]]:
    """Return recent BigQuery COPY jobs with a compact monitoring payload."""

    rows: list[dict[str, Any]] = []
    for job in client.list_jobs():
        job_type = getattr(job, "job_type", None)

        if str(job_type).upper() != "COPY":
            continue

        rows.append(
            {
                "job_id": job.job_id,
                "state": job.state,
                "location": getattr(job, "location", None),
                "created": getattr(job, "created", None),
                "ended": getattr(job, "ended", None),
                "user_email": getattr(job, "user_email", None),
                "bytes_processed": getattr(job, "total_bytes_processed", None),
                "bytes_billed": getattr(job, "total_bytes_billed", None),
                "type": str(job_type).upper(),
            }
        )

    return rows


def show_recent_copy_jobs(
    client: bigquery.Client,
) -> None:
    """Display recent BigQuery COPY jobs."""

    jobs = list_recent_copy_jobs(client)
    if not jobs:
        print(f"No recent BigQuery COPY jobs found for project {client.project}.")
        return

    headers = [
        "Job Type",
        "State",
        "Start Time",
        "End Time",
        "User Email",
        "Job ID",
    ]
    rows = []
    for job in jobs:
        rows.append(
            [
                job["type"],
                job["state"],
                job["created"],
                job["ended"],
                job["user_email"],
                job["job_id"],
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))

    def render_line(values: list[str]) -> str:
        return "  " + "   ".join(
            str(value).ljust(widths[index]) for index, value in enumerate(values)
        )

    print(render_line(headers))
    print("  " + "   ".join("-" * widths[index] for index in range(len(headers))))
    for row in rows:
        print(render_line([str(value) for value in row]))


def main() -> None:
    """Copy data from one BigQuery dataset to another based on the migrated DAG config."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )

    parser = argparse.ArgumentParser(prog="bigquery_copy_datas")
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
        "--job-log-dataset",
        help="Dedicated BigQuery dataset in the destination project used to persist copy-job logs.",
    )
    parser.add_argument(
        "--job-log-table",
        default=DEFAULT_JOB_LOG_TABLE_ID,
        help="BigQuery table used to persist copy-job logs.",
    )
    parser.add_argument(
        "--job-log-location",
        help="BigQuery location for the logging dataset if it must be created. Defaults to the destination dataset location.",
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
    job_log_config = None
    if args.job_log_dataset:
        job_log_config = JobLogConfig(
            dataset_id=args.job_log_dataset,
            table_id=args.job_log_table,
            location=args.job_log_location,
        )
    copy_dag_tables(
        env_config,
        args.dag_name,
        args.env,
        dag_copy_config,
        job_log_config,
    )


if __name__ == "__main__":
    main()
