import logging
from datetime import datetime

from airflow.models import DagModel, DagRun, TaskInstance
from airflow.utils.session import provide_session
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID
from dependencies.airflow_dag_metrics.config import BQ_SCHEMA, FULL_TABLE_ID
from google.cloud import bigquery
from sqlalchemy import func

logger = logging.getLogger(__name__)

KNOWN_TASK_STATES = {"success", "failed", "skipped", "upstream_failed"}


@provide_session
def collect_airflow_metrics(snapshot_date: str, session=None):
    """Collect DAG metrics from Airflow metadata DB and write to BigQuery."""
    today = datetime.strptime(snapshot_date, "%Y-%m-%d").date()

    active_dags = (
        session.query(DagModel)
        .filter(
            DagModel.is_active.is_(True),
            DagModel.is_paused.is_(False),
            DagModel.schedule_interval.isnot(None),
        )
        .all()
    )

    logger.info("Found %d active scheduled DAGs", len(active_dags))

    rows = []
    for dag in active_dags:
        dag_id = dag.dag_id

        last_run = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id)
            .order_by(DagRun.execution_date.desc())
            .first()
        )

        last_run_state = last_run.state if last_run else None
        last_run_execution_date = last_run.execution_date if last_run else None

        task_counts = {"success": 0, "failed": 0, "skipped": 0, "upstream_failed": 0}
        if last_run:
            task_states = (
                session.query(TaskInstance.state, func.count())
                .filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == last_run.run_id,
                )
                .group_by(TaskInstance.state)
                .all()
            )
            for state, count in task_states:
                if state in KNOWN_TASK_STATES:
                    task_counts[state] = count

        tags = [tag.name for tag in dag.tags] if dag.tags else []

        rows.append(
            {
                "snapshot_date": today.isoformat(),
                "environment": ENV_SHORT_NAME,
                "dag_id": dag_id,
                "tags": tags,
                "schedule_interval": str(dag.schedule_interval),
                "dag_run_state": last_run_state,
                "dag_run_execution_date": last_run_execution_date.isoformat()
                if last_run_execution_date
                else None,
                "tasks_success": task_counts["success"],
                "tasks_failed": task_counts["failed"],
                "tasks_skipped": task_counts["skipped"],
                "tasks_upstream_failed": task_counts["upstream_failed"],
            }
        )

    _write_to_bigquery(rows, today)
    logger.info("Wrote %d rows to %s for %s", len(rows), FULL_TABLE_ID, today)


def _write_to_bigquery(rows, snapshot_date):
    """Write rows to BigQuery, replacing data for the given snapshot_date partition."""
    if not rows:
        logger.warning("No rows to write")
        return

    client = bigquery.Client(project=GCP_PROJECT_ID)

    table = bigquery.Table(FULL_TABLE_ID, schema=BQ_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="snapshot_date",
    )
    client.create_table(table, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date",
        ),
    )

    partition_id = snapshot_date.strftime("%Y%m%d")
    destination = f"{FULL_TABLE_ID}${partition_id}"

    job = client.load_table_from_json(rows, destination, job_config=job_config)
    job.result()
