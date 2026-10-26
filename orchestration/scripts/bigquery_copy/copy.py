from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from google.cloud import bigquery

from .config import (
    COPY_JOB_POLL_INTERVAL_SECONDS,
    ESTIMATED_TRANSFER_USD_PER_GIB,
    CopyPlan,
    DagCopyConfig,
    EnvironmentConfig,
    JobLogConfig,
    TableCopyEstimate,
)
from .monitoring import (
    build_copy_log_row,
    ensure_job_log_table,
    write_copy_job_log,
)

LOGGER = logging.getLogger(__name__)


def estimate_table_copy_cost(table: bigquery.Table) -> tuple[int, float]:
    """Return the estimated bytes and USD price for one table copy."""

    estimated_bytes = table.num_bytes or 0
    estimated_price_usd = (estimated_bytes / 1024**3) * ESTIMATED_TRANSFER_USD_PER_GIB
    return estimated_bytes, estimated_price_usd


def build_copy_plan(
    client: bigquery.Client,
    environment: EnvironmentConfig,
    dag_copy_config: DagCopyConfig,
) -> CopyPlan:
    """Build the full copy plan for a DAG, with a per-table cost estimate."""

    source_dataset_id = environment.source_dataset_id(dag_copy_config.source_dataset)
    destination_dataset_id = environment.destination_dataset_id(
        dag_copy_config.source_dataset
    )
    source_dataset_obj = client.get_dataset(
        f"{environment.source_project}.{source_dataset_id}"
    )
    destination_dataset_obj = client.get_dataset(
        f"{environment.destination_project}.{destination_dataset_id}"
    )

    tables: list[TableCopyEstimate] = []
    for table_name in dag_copy_config.tables_to_copy:
        source_ref = bigquery.TableReference.from_string(
            f"{environment.source_project}.{source_dataset_id}.{table_name}"
        )
        source_table = client.get_table(source_ref)
        estimated_bytes, estimated_price_usd = estimate_table_copy_cost(source_table)
        tables.append(
            TableCopyEstimate(
                table_name=table_name,
                row_count=source_table.num_rows,
                estimated_bytes=estimated_bytes,
                estimated_price_usd=estimated_price_usd,
            )
        )

    return CopyPlan(
        tables=tables,
        source_location=source_dataset_obj.location,
        destination_location=destination_dataset_obj.location,
    )


def display_copy_plan(dag_name: str, plan: CopyPlan) -> None:
    """Print a detailed, per-table cost breakdown of the planned copy."""

    print(
        f"\nCopy plan for DAG '{dag_name}' "
        f"({plan.source_location} -> {plan.destination_location}):"
    )
    print(f"{'TABLE':<40} {'ROWS':>15} {'BYTES':>18} {'EST. USD':>12}")
    print("-" * 87)
    for table in plan.tables:
        print(
            f"{table.table_name:<40} "
            f"{table.row_count:>15,} "
            f"{table.estimated_bytes:>18,} "
            f"{table.estimated_price_usd:>12.4f}"
        )
    print("-" * 87)
    print(
        f"{'TOTAL (' + str(len(plan.tables)) + ' tables)':<40} "
        f"{plan.total_rows:>15,} "
        f"{plan.total_bytes:>18,} "
        f"{plan.total_price_usd:>12.4f}"
    )
    print(
        f"Pricing transfer {plan.source_location} -> {plan.destination_location} "
        f"= {ESTIMATED_TRANSFER_USD_PER_GIB:.2f} $/GiB.\n"
    )


def confirm_copy_plan(assume_yes: bool) -> bool:
    """Ask the user to confirm the copy unless ``assume_yes`` is set."""

    if assume_yes:
        return True

    answer = input("Proceed with the copy? [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def wait_for_job(job: bigquery.job.CopyJob) -> None:
    """Poll a BigQuery job until completion and log its state along the way."""

    while not job.done():
        time.sleep(COPY_JOB_POLL_INTERVAL_SECONDS)
        job.reload()
        LOGGER.info("BigQuery copy job %s state=%s", job.job_id, job.state)

    job.result()
    LOGGER.info(
        "BigQuery copy job %s finished with state=%s; manual status check: bq show --job --project_id=%s --location=%s %s",
        job.job_id,
        job.state,
        job.project,
        getattr(job, "location", "unknown"),
        job.job_id,
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


def copy_table(
    client: bigquery.Client,
    environment: EnvironmentConfig,
    dag_name: str,
    source_dataset: str,
    table_name: str,
    source_location: str,
    destination_location: str,
    job_log_table: bigquery.TableReference | None = None,
) -> None:
    """Copy one table from the source project to the destination project."""

    started_at = time.monotonic()
    started_at_utc = datetime.now(timezone.utc)
    source_dataset_id = environment.source_dataset_id(source_dataset)
    destination_dataset_id = environment.destination_dataset_id(source_dataset)

    source_ref = bigquery.TableReference.from_string(
        f"{environment.source_project}.{source_dataset_id}.{table_name}"
    )
    destination_ref = bigquery.TableReference.from_string(
        f"{environment.destination_project}.{destination_dataset_id}.{table_name}"
    )

    copy_job: bigquery.job.CopyJob | None = None
    source_row_count: int | None = None
    destination_row_count: int | None = None
    estimated_bytes = 0
    estimated_price_usd = 0.0
    copy_status = "FAILED"
    error_message: str | None = None

    try:
        source_table = client.get_table(source_ref)
        source_row_count = source_table.num_rows
        estimated_bytes, estimated_price_usd = estimate_table_copy_cost(source_table)

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

        if job_log_table is not None:
            try:
                write_copy_job_log(
                    client,
                    job_log_table,
                    build_copy_log_row(
                        dag_name=dag_name,
                        copy_status="STARTED",
                        job_id=copy_job.job_id,
                        source_ref=source_ref,
                        destination_ref=destination_ref,
                        source_row_count=source_row_count,
                        destination_row_count=None,
                        estimated_bytes=estimated_bytes,
                        estimated_price_usd=estimated_price_usd,
                        started_at_utc=started_at_utc,
                    ),
                )
                LOGGER.info(
                    "Persisted STARTED copy log row for job %s",
                    copy_job.job_id,
                )
            except Exception:
                LOGGER.exception(
                    "Failed to persist STARTED copy log for %s.%s",
                    source_ref.dataset_id,
                    table_name,
                )

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
            try:
                write_copy_job_log(
                    client,
                    job_log_table,
                    build_copy_log_row(
                        dag_name=dag_name,
                        copy_status=copy_status,
                        job_id=getattr(copy_job, "job_id", None),
                        source_ref=source_ref,
                        destination_ref=destination_ref,
                        source_row_count=source_row_count,
                        destination_row_count=destination_row_count,
                        estimated_bytes=estimated_bytes,
                        estimated_price_usd=estimated_price_usd,
                        started_at_utc=started_at_utc,
                        ended_at_utc=ended_at_utc,
                        duration_seconds=elapsed_seconds,
                        error_message=error_message,
                    ),
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


def copy_dag_tables(
    environment: EnvironmentConfig,
    dag_name: str,
    dag_copy_config: DagCopyConfig,
    job_log_config: JobLogConfig | None = None,
    assume_yes: bool = False,
) -> None:
    """Copy every table declared for a migrated DAG."""

    if not dag_copy_config.tables_to_copy:
        LOGGER.info("No tables to copy for this DAG.")
        return

    client = bigquery.Client(project=environment.destination_project)
    plan = build_copy_plan(client, environment, dag_copy_config)
    display_copy_plan(dag_name, plan)

    if not confirm_copy_plan(assume_yes):
        LOGGER.info("Copy aborted by user.")
        return

    source_location = plan.source_location
    destination_location = plan.destination_location

    job_log_table = ensure_job_log_table(
        client,
        environment.destination_project,
        job_log_config or JobLogConfig(),
    )

    failed_tables: list[str] = []
    for table_name in dag_copy_config.tables_to_copy:
        LOGGER.info(
            "Copying %s.%s (%s -> %s)",
            dag_copy_config.source_dataset,
            table_name,
            environment.source_project,
            environment.destination_project,
        )
        try:
            copy_table(
                client,
                environment,
                dag_name,
                dag_copy_config.source_dataset,
                table_name,
                source_location,
                destination_location,
                job_log_table,
            )
        except Exception:
            LOGGER.exception(
                "Failed to copy %s.%s; continuing with remaining tables.",
                dag_copy_config.source_dataset,
                table_name,
            )
            failed_tables.append(table_name)

    if failed_tables:
        raise RuntimeError(
            f"Copy failed for {len(failed_tables)} table(s): {', '.join(failed_tables)}"
        )
