import datetime

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import bigquery_job_task
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
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

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

DAG_NAME = "import_brevo_v2"
GCE_INSTANCE = f"import-brevo-2-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/"
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import brevo tables",
    schedule_interval=get_airflow_schedule(
        "00 03 * * *" if ENV_SHORT_NAME == "prod" else None
    ),
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
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"job_type": "long_task", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        requirement_file="jobs/brevo/requirements.txt",
        branch="{{ params.branch }}",
        python_version="'3.10'",
        base_dir=BASE_PATH,
        retries=2,
    )

    import_pro_transactional_data_to_tmp = SSHGCEOperator(
        task_id="import_pro_transactional_data_to_tmp",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="""
            python -m jobs.brevo.main \
            --target transactional \
            --audience pro \
            --start-date {{ params.start_date }} \
            --end-date {{ params.end_date }}{% if params['async'] %} \
            --async --max-concurrent {{ params.async_concurent }}{% endif %}
        """,
        do_xcom_push=True,
    )

    import_native_transactional_data_to_tmp = SSHGCEOperator(
        task_id="import_native_transactional_data_to_tmp",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="""
            python -m jobs.brevo.main \
            --target transactional \
            --audience native \
            --start-date {{ params.start_date }} \
            --end-date {{ params.end_date }}{% if params['async'] %} \
            --async --max-concurrent {{ params.async_concurent }}{% endif %}
        """,
        do_xcom_push=True,
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

    import_pro_newsletter_data_to_raw = SSHGCEOperator(
        task_id="import_pro_newsletter_data_to_raw",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python -m jobs.brevo.main --target newsletter --audience pro --start-date {{ params.start_date }} --end-date {{ params.end_date }}",
        do_xcom_push=True,
    )

    import_native_newsletter_data_to_raw = SSHGCEOperator(
        task_id="import_native_newsletter_data_to_raw",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python -m jobs.brevo.main --target newsletter --audience native --start-date {{ params.start_date }} --end-date {{ params.end_date }}",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
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

    (gce_instance_start >> fetch_install_code)

    (
        fetch_install_code
        >> import_pro_transactional_data_to_tmp
        >> import_pro_newsletter_data_to_raw
        >> gce_instance_stop
    )

    (
        fetch_install_code
        >> import_native_transactional_data_to_tmp
        >> import_native_newsletter_data_to_raw
        >> gce_instance_stop
    )

    (
        gce_instance_stop
        >> end_job
        >> raw_table_tasks
        >> end_raw
        >> clean_table_tasks
        >> end_clean
        >> analytics_table_tasks
    )
