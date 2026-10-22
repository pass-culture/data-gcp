from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .config import JobLogConfig

LOGGER = logging.getLogger(__name__)

JOB_LOG_TABLE_SCHEMA = [
    bigquery.SchemaField("logged_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("dag_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("copy_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("job_id", "STRING"),
    bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("destination_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_count", "INTEGER"),
    bigquery.SchemaField("destination_row_count", "INTEGER"),
    bigquery.SchemaField("estimated_bytes", "INTEGER"),
    bigquery.SchemaField("estimated_price_usd", "FLOAT"),
    bigquery.SchemaField("copy_started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("copy_ended_at", "TIMESTAMP"),
    bigquery.SchemaField("duration_seconds", "FLOAT"),
    bigquery.SchemaField("error_message", "STRING"),
]


def to_rfc3339(value: datetime) -> str:
    """Convert a datetime to RFC3339 for BigQuery JSON inserts."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc).isoformat()


def ensure_job_log_table(
    client: bigquery.Client,
    project_id: str,
    config: JobLogConfig,
) -> bigquery.TableReference:
    """Create the logging table if it does not exist yet.

    The dataset is expected to already exist and to be created manually.
    """

    dataset_ref = bigquery.DatasetReference(project_id, config.dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, config.table_id)

    try:
        client.get_dataset(dataset_ref)
    except NotFound as exc:
        raise ValueError(
            f"BigQuery dataset {dataset_ref.path} does not exist. "
            "Create it before running the copy job."
        ) from exc

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

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(
            f"Failed to insert copy-job log row into {table_ref.path}: {errors}"
        )


def build_copy_log_row(
    *,
    dag_name: str,
    copy_status: str,
    job_id: str | None,
    source_ref: bigquery.TableReference,
    destination_ref: bigquery.TableReference,
    source_row_count: int | None,
    destination_row_count: int | None,
    estimated_bytes: int,
    estimated_price_usd: float,
    started_at_utc: datetime,
    ended_at_utc: datetime | None = None,
    duration_seconds: float | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build a copy-job monitoring row matching ``JOB_LOG_TABLE_SCHEMA``."""

    return {
        "logged_at": to_rfc3339(datetime.now(timezone.utc)),
        "dag_name": dag_name,
        "copy_status": copy_status,
        "job_id": job_id,
        "source_table": source_ref.table_id,
        "destination_table": destination_ref.table_id,
        "source_row_count": source_row_count,
        "destination_row_count": destination_row_count,
        "estimated_bytes": estimated_bytes,
        "estimated_price_usd": estimated_price_usd,
        "copy_started_at": to_rfc3339(started_at_utc),
        "copy_ended_at": to_rfc3339(ended_at_utc) if ended_at_utc else None,
        "duration_seconds": duration_seconds,
        "error_message": error_message,
    }


def show_recent_copy_jobs(client: bigquery.Client) -> None:
    """Print recent BigQuery COPY jobs for manual follow-up."""

    copy_jobs = [
        job
        for job in client.list_jobs()
        if str(getattr(job, "job_type", "")).upper() == "COPY"
    ]
    if not copy_jobs:
        print(f"No recent BigQuery COPY jobs found for project {client.project}.")
        return

    print(f"{'STATE':<10} {'CREATED':<28} {'USER':<30} JOB ID")
    for job in copy_jobs:
        print(
            f"{str(job.state):<10} "
            f"{str(getattr(job, 'created', '')):<28} "
            f"{str(getattr(job, 'user_email', '')):<30} "
            f"{job.job_id}"
        )
