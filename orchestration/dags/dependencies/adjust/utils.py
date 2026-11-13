import datetime
import hashlib
import logging
import os
from pathlib import Path

from airflow.exceptions import AirflowException
from dependencies.adjust.import_adjust import (
    COPY_DATASET,
    DESTINATION_LOCATION,
    EXPECTED_SCHEMA,
    PUBLISH_DATASET,
    SOURCE_DATASET,
    SOURCE_LOCATION,
    SOURCE_PROJECT,
)
from jinja2 import Environment, FileSystemLoader, StrictUndefined

logger = logging.getLogger(__name__)


def render_sql(template_name, **context):
    environment = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "sql" / "raw"),
        undefined=StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
    )
    return environment.get_template(template_name).render(
        expected_schema=EXPECTED_SCHEMA, **context
    )


def get_bigquery_client(location=SOURCE_LOCATION):
    if os.environ.get("ENV_SHORT_NAME", "dev") != "dev":
        raise AirflowException("The Adjust import prototype is restricted to dev")

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        location=location,
    )
    return hook.get_client(
        project_id=os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp"),
        location=location,
    )


def discover_and_validate_adjust_tables(export_date):
    from google.cloud import bigquery

    export_date = datetime.date.fromisoformat(export_date).isoformat()
    logger.info(
        "Discovering Adjust tables in %s.%s for %s",
        SOURCE_PROJECT,
        SOURCE_DATASET,
        export_date,
    )
    client = get_bigquery_client()
    query = render_sql(
        "discover_tables.sql",
        source_project=SOURCE_PROJECT,
        source_dataset=SOURCE_DATASET,
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_prefix", "STRING", f"{export_date}_")
        ]
    )
    rows = client.query(query, job_config=job_config, location=SOURCE_LOCATION).result()
    table_names = [row.table_name for row in rows]
    if not table_names:
        raise AirflowException(
            f"No Adjust tables found for {export_date} in {SOURCE_PROJECT}.{SOURCE_DATASET}"
        )

    logger.info("Found %s Adjust tables for %s", len(table_names), export_date)
    for table_name in table_names:
        logger.info(
            "Source table: %s.%s.%s", SOURCE_PROJECT, SOURCE_DATASET, table_name
        )

    type_aliases = {"INTEGER": "INT64", "FLOAT": "FLOAT64"}
    expected_schema = {
        column_name: (data_type, "NULLABLE")
        for column_name, data_type in EXPECTED_SCHEMA.items()
    }
    for table_name in table_names:
        table_id = f"{SOURCE_PROJECT}.{SOURCE_DATASET}.{table_name}"
        table = client.get_table(table_id)
        actual_schema = {
            field.name: (
                type_aliases.get(field.field_type, field.field_type),
                field.mode,
            )
            for field in table.schema
        }
        missing_fields = sorted(expected_schema.keys() - actual_schema.keys())
        extra_fields = sorted(actual_schema.keys() - expected_schema.keys())
        incompatible_fields = {
            column_name: {
                "expected": expected_schema[column_name],
                "actual": actual_schema[column_name],
            }
            for column_name in sorted(expected_schema.keys() & actual_schema.keys())
            if actual_schema[column_name] != expected_schema[column_name]
        }
        if missing_fields or extra_fields or incompatible_fields:
            raise AirflowException(
                f"Invalid Adjust schema for {table_id}: missing={missing_fields}, "
                f"extra={extra_fields}, incompatible={incompatible_fields}"
            )
        logger.info("Schema valid for %s (%s columns)", table_id, len(actual_schema))

    logger.info("Schema validated for %s Adjust tables", len(table_names))
    return table_names


def stage_and_validate_adjust_tables(table_names, export_date, dag_id, run_id):
    from google.cloud import bigquery

    if not table_names:
        raise AirflowException("No Adjust tables provided for staging")

    export_date = datetime.date.fromisoformat(export_date).isoformat()
    run_suffix = hashlib.sha256(f"{dag_id}:{run_id}".encode()).hexdigest()[:16]
    staging_table_id = (
        f"{SOURCE_PROJECT}.{SOURCE_DATASET}."
        f"adjust_staging_{export_date.replace('-', '')}_{run_suffix}"
    )
    query_parameters = []
    for table_index, table_name in enumerate(table_names):
        if "`" in table_name:
            raise AirflowException(f"Invalid Adjust table name: {table_name}")
        parameter_name = f"source_table_{table_index}"
        query_parameters.append(
            bigquery.ScalarQueryParameter(parameter_name, "STRING", table_name)
        )
    query = render_sql(
        "stage_tables.sql",
        staging_table_id=staging_table_id,
        source_project=SOURCE_PROJECT,
        source_dataset=SOURCE_DATASET,
        table_names=table_names,
    )
    logger.info("Staging %s Adjust tables into %s", len(table_names), staging_table_id)
    logger.info("Staging SQL:\n%s", query)
    client = get_bigquery_client()
    client.query(
        query,
        job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
        location=SOURCE_LOCATION,
    ).result()
    staging_table = client.get_table(staging_table_id)
    logger.info(
        "Staged %s rows in %s; expiration: %s",
        staging_table.num_rows,
        staging_table_id,
        staging_table.expires,
    )
    staging_result = {"table_id": staging_table_id, "row_count": staging_table.num_rows}
    validate_adjust_staging(staging_result, table_names, export_date)
    return staging_result


def validate_adjust_staging(staging_result, table_names, export_date):
    from google.cloud import bigquery

    if not table_names:
        raise AirflowException("No Adjust tables provided for staging validation")

    expected_day = datetime.date.fromisoformat(export_date)
    staging_table_id = staging_result["table_id"]
    if "`" in staging_table_id or not staging_table_id.startswith(
        f"{SOURCE_PROJECT}.{SOURCE_DATASET}.adjust_staging_"
    ):
        raise AirflowException(f"Unexpected Adjust staging table: {staging_table_id}")

    query_parameters = [
        bigquery.ScalarQueryParameter("expected_day", "DATE", expected_day),
        bigquery.ArrayQueryParameter("source_table_names", "STRING", table_names),
    ]
    query = render_sql(
        "validate_staging.sql",
        staging_table_id=staging_table_id,
    )
    client = get_bigquery_client()
    rows = list(
        client.query(
            query,
            job_config=bigquery.QueryJobConfig(query_parameters=query_parameters),
            location=SOURCE_LOCATION,
        ).result()
    )
    if not rows:
        raise AirflowException("No results returned for Adjust staging validation")

    summary = rows[0]
    if summary.invalid_day_rows or summary.invalid_source_rows:
        raise AirflowException(
            "Adjust staging contains invalid dates or unknown source tables"
        )
    if summary.row_count != staging_result["row_count"]:
        raise AirflowException("Adjust staging row count changed since loading")

    logger.info(
        "Staging validation passed: %s rows in %s",
        summary.row_count,
        staging_table_id,
    )
    return staging_result


def copy_adjust_staging(staging_result):
    from google.cloud import bigquery

    source = bigquery.TableReference.from_string(staging_result["table_id"])
    if (
        source.project != SOURCE_PROJECT
        or source.dataset_id != SOURCE_DATASET
        or not source.table_id.startswith("adjust_staging_")
    ):
        raise AirflowException(
            f"Unexpected Adjust copy source: {staging_result['table_id']}"
        )

    destination = bigquery.DatasetReference(SOURCE_PROJECT, COPY_DATASET).table(
        source.table_id
    )
    destination_table_id = (
        f"{destination.project}.{destination.dataset_id}.{destination.table_id}"
    )
    logger.info(
        "Copying validated Adjust staging %s (%s) to %s (%s)",
        staging_result["table_id"],
        SOURCE_LOCATION,
        destination_table_id,
        DESTINATION_LOCATION,
    )
    client = get_bigquery_client()
    client.copy_table(
        source,
        destination,
        job_config=bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        ),
        location=SOURCE_LOCATION,
    ).result()
    copied_table = client.get_table(destination)
    if copied_table.location != DESTINATION_LOCATION:
        raise AirflowException(
            f"Unexpected Adjust copy location: {copied_table.location}"
        )
    if copied_table.num_rows != staging_result["row_count"]:
        raise AirflowException(
            f"Adjust copy row count mismatch: expected={staging_result['row_count']}, "
            f"copied={copied_table.num_rows}"
        )

    logger.info(
        "Copy validated: %s rows in %s; location: %s; expiration: %s",
        copied_table.num_rows,
        destination_table_id,
        copied_table.location,
        copied_table.expires,
    )
    return {"table_id": destination_table_id, "row_count": copied_table.num_rows}


def publish_adjust_partition(copy_result, export_date, dag_id):
    from google.cloud import bigquery

    expected_day = datetime.date.fromisoformat(export_date)
    source = bigquery.TableReference.from_string(copy_result["table_id"])
    if (
        source.project != SOURCE_PROJECT
        or source.dataset_id != COPY_DATASET
        or not source.table_id.startswith("adjust_staging_")
        or "`" in source.table_id
    ):
        raise AirflowException(
            f"Unexpected Adjust publication source: {copy_result['table_id']}"
        )
    if copy_result["row_count"] <= 0:
        raise AirflowException("Refusing to publish an empty Adjust day")

    table_suffix = hashlib.sha256(dag_id.encode()).hexdigest()[:12]
    table_name = f"adjust_daily_test_{table_suffix}"
    destination_table_id = f"{SOURCE_PROJECT}.{PUBLISH_DATASET}.{table_name}"
    destination_partition = f"{destination_table_id}${expected_day.strftime('%Y%m%d')}"
    client = get_bigquery_client(location=DESTINATION_LOCATION)
    logger.info(
        "Publishing %s rows to %s", copy_result["row_count"], destination_partition
    )
    client.query(
        render_sql("publish_partition.sql", table_id=copy_result["table_id"]),
        job_config=bigquery.QueryJobConfig(
            destination=destination_partition,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="day",
            ),
        ),
        location=DESTINATION_LOCATION,
    ).result()

    logger.info(
        "Published %s rows for %s in %s; only this partition was replaced",
        copy_result["row_count"],
        expected_day,
        destination_table_id,
    )
    return {"table_id": destination_table_id, "row_count": copy_result["row_count"]}
