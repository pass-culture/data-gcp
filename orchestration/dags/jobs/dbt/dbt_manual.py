from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import datetime, timedelta

default_args = {
    "start_date": datetime(2020, 12, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "dbt_manual_command",
    default_args=default_args,
    catchup=False,
    description="A dbt wrapper for airflow",
    schedule_interval=None,
    params={
        "dbt_command": Param(
            "compile",
            enum=[
                "compile",
                "docs generate",
                "parse",
                "deps",
                "clean",
                "run",
                "test",
                "snapshot",
                "seed",
            ],
        ),
        "target": Param(
            ENV_SHORT_NAME,
            enum=["None", "dev", "stg", "prod"],
        ),
        "source_env": Param(
            ENV_SHORT_NAME,
            enum=["None", "dev", "stg", "prod"],
        ),
    },
    tags=[DAG_TAGS.DBT.value, DAG_TAGS.DE.value],
)

start = EmptyOperator(task_id="start", dag=dag)

dbt_manual_command = BashOperator(
    task_id="dbt_manual_command",
    bash_command="""
    dbt {{ params.dbt_command }} \
    {% if params.target != "None" %} -t {{ params.target }}{% endif %} \
    {% if params.source_env != "None" %} --vars "{'ENV_SHORT_NAME':'{{ params.source_env }}'}"{% endif %}
    """,
    cwd=PATH_TO_DBT_PROJECT,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> dbt_manual_command >> end
