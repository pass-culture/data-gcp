import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import (
    depends_loop,
    get_airflow_schedule,
)
from dependencies.brevo.import_brevo import (
    analytics_tables,
    clean_tables,
    raw_tables,
)

DAG_NAME = "import_brevo_v2"
MICROSERVICE_PATH = "jobs/etl_jobs/jobs/brevo"
SPARSE_PATHS = [
    MICROSERVICE_PATH,
    "jobs/etl_jobs/connectors",
    "jobs/etl_jobs/http_tools",
]
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import brevo tables",
    schedule=get_airflow_schedule("00 03 * * *" if ENV_SHORT_NAME == "prod" else None),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    render_template_as_native_obj=True,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "start_date": Param(
            default=yesterday,
            type="string",
        ),
        "end_date": Param(
            default=yesterday,
            type="string",
        ),
        "async": Param(default=True, type="boolean"),
        "async_concurent": Param(default=5, type="integer"),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
) as dag:
    _kpo_common = {
        "orchestration_mode": "celery",
        "queue": "k8s-watcher",
        "runtime_mode": "gitsynced",
        "runtime_branch": "{{ params.branch }}",
        "runtime_image": "py313",
        "runtime_image_tag": "v1",
        "microservice_path": MICROSERVICE_PATH,
        "runtime_sparse_paths": SPARSE_PATHS,
        "runtime_workdir": MICROSERVICE_PATH,
        "env_vars": {"PYTHONPATH": "/app/jobs/etl_jobs", "GCP_PROJECT": GCP_PROJECT_ID},
        "container_resources": DEFAULT_CONTAINER_RESOURCES,
    }

    import_pro_transactional_data_to_tmp = CustomKubernetesPodOperator(
        task_id="import_pro_transactional_data_to_tmp",
        arguments=[
            "main.py",
            "--target",
            "transactional",
            "--audience",
            "pro",
            "--start-date",
            "{{ params.start_date }}",
            "--end-date",
            "{{ params.end_date }}",
            "{{ '--async --max-concurrent ' + params.async_concurent|string if params['async'] else '' }}",
        ],
        do_xcom_push=True,
        **_kpo_common,
    )

    import_native_transactional_data_to_tmp = CustomKubernetesPodOperator(
        task_id="import_native_transactional_data_to_tmp",
        arguments=[
            "main.py",
            "--target",
            "transactional",
            "--audience",
            "native",
            "--start-date",
            "{{ params.start_date }}",
            "--end-date",
            "{{ params.end_date }}",
            "{{ '--async --max-concurrent ' + params.async_concurent|string if params['async'] else '' }}",
        ],
        do_xcom_push=True,
        **_kpo_common,
    )

    ### jointure avec pcapi pour retirer les emails
    raw_table_jobs = {}

    for name, params in raw_tables.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)
        raw_table_jobs[name] = {
            "operator": task,
        }

    end_job = EmptyOperator(task_id="end_job", dag=dag)
    end_raw = EmptyOperator(task_id="end_raw", dag=dag)

    raw_table_tasks = depends_loop(
        raw_tables,
        raw_table_jobs,
        end_job,
        dag=dag,
        default_end_operator=end_raw,
    )

    import_pro_newsletter_data_to_raw = CustomKubernetesPodOperator(
        task_id="import_pro_newsletter_data_to_raw",
        arguments=[
            "main.py",
            "--target",
            "newsletter",
            "--audience",
            "pro",
            "--start-date",
            "{{ params.start_date }}",
            "--end-date",
            "{{ params.end_date }}",
        ],
        do_xcom_push=True,
        **_kpo_common,
    )

    import_native_newsletter_data_to_raw = CustomKubernetesPodOperator(
        task_id="import_native_newsletter_data_to_raw",
        arguments=[
            "main.py",
            "--target",
            "newsletter",
            "--audience",
            "native",
            "--start-date",
            "{{ params.start_date }}",
            "--end-date",
            "{{ params.end_date }}",
        ],
        do_xcom_push=True,
        **_kpo_common,
    )

    clean_table_jobs = {}

    for name, params in clean_tables.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)
        clean_table_jobs[name] = {
            "operator": task,
        }

    end_clean = EmptyOperator(task_id="end_clean", dag=dag)

    clean_table_tasks = depends_loop(
        clean_tables,
        clean_table_jobs,
        end_raw,
        dag=dag,
        default_end_operator=end_clean,
    )

    analytics_table_jobs = {}
    for name, params in analytics_tables.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)
        analytics_table_jobs[name] = {
            "operator": task,
            "depends": params.get("depends", []),
            "dag_depends": params.get("dag_depends", []),
        }

        # import_tables_to_analytics_tasks.append(task)

    end = EmptyOperator(task_id="end", dag=dag)
    analytics_table_tasks = depends_loop(
        analytics_tables,
        analytics_table_jobs,
        end_clean,
        dag=dag,
        default_end_operator=end,
    )

    (
        import_pro_transactional_data_to_tmp
        >> import_pro_newsletter_data_to_raw
        >> end_job
    )

    (
        import_native_transactional_data_to_tmp
        >> import_native_newsletter_data_to_raw
        >> end_job
    )

    (
        end_job
        >> raw_table_tasks
        >> end_raw
        >> clean_table_tasks
        >> end_clean
        >> analytics_table_tasks
    )
