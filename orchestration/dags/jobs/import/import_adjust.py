import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from common.alerts.task_fail import task_fail_slack_alert
from common.config import DAG_TAGS
from dependencies.adjust.utils import (
    copy_adjust_staging,
    discover_and_validate_adjust_tables,
    publish_adjust_partition,
    stage_and_validate_adjust_tables,
)

DAG_NAME = "import_adjust"


default_dag_args = {
    "start_date": datetime.datetime(2026, 3, 6, tzinfo=datetime.timezone.utc),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Consolidate daily Adjust exports",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=datetime.timedelta(minutes=10),
    params={
        "export_date": Param(
            type="string",
            format="date",
            description="Adjust export date in YYYY-MM-DD format",
        ),
    },
    tags=[DAG_TAGS.DE.value, "adjust"],
) as dag:
    start = EmptyOperator(task_id="start")
    prepare_tables = PythonOperator(
        task_id="prepare_tables",
        python_callable=discover_and_validate_adjust_tables,
        op_kwargs={"export_date": "{{ params.export_date }}"},
    )
    stage_and_validate = PythonOperator(
        task_id="stage_and_validate",
        python_callable=stage_and_validate_adjust_tables,
        op_kwargs={
            "table_names": prepare_tables.output,
            "export_date": "{{ params.export_date }}",
            "dag_id": "{{ dag.dag_id }}",
            "run_id": "{{ run_id }}",
        },
    )
    copy_to_tmp = PythonOperator(
        task_id="copy_to_tmp",
        python_callable=copy_adjust_staging,
        op_kwargs={"staging_result": stage_and_validate.output},
    )
    publish_partition = PythonOperator(
        task_id="publish_partition",
        python_callable=publish_adjust_partition,
        op_kwargs={
            "copy_result": copy_to_tmp.output,
            "export_date": "{{ params.export_date }}",
            "dag_id": "{{ dag.dag_id }}",
        },
    )
    end = EmptyOperator(task_id="end")

    (
        start
        >> prepare_tables
        >> stage_and_validate
        >> copy_to_tmp
        >> publish_partition
        >> end
    )
