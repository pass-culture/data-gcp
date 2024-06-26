import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    DAG_FOLDER,
)
from common.operators.biquery import bigquery_job_task
from common.utils import (
    depends_loop,
    get_airflow_schedule,
)

from common.alerts import task_fail_slack_alert

from common import macros

from dependencies.sendinblue.import_sendinblue import (
    raw_tables,
    clean_tables,
    analytics_tables,
)


GCE_INSTANCE = f"import-sendinblue-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/sendinblue"
yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    "import_sendinblue",
    default_args=default_dag_args,
    description="Import sendinblue tables",
    schedule_interval=get_airflow_schedule("00 04 * * *")
    if ENV_SHORT_NAME in ["prod", "stg"]
    else get_airflow_schedule("00 07 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
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
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.9",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    import_transactional_data_to_tmp = SSHGCEOperator(
        task_id="import_transactional_data_to_tmp",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command='python main.py --target transactional --start-date "{{ params.start_date }}" --end-date "{{ params.end_date }}"',
        do_xcom_push=True,
    )

    ### jointure avec pcapi pour retirer les emails
    raw_table_jobs = {}

    for name, params in raw_tables.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)
        raw_table_jobs[name] = {
            "operator": task,
        }

    end_raw = DummyOperator(task_id="end_raw", dag=dag)

    raw_table_tasks = depends_loop(
        raw_tables,
        raw_table_jobs,
        import_transactional_data_to_tmp,
        dag=dag,
        default_end_operator=end_raw,
    )

    import_newsletter_data_to_raw = SSHGCEOperator(
        task_id="import_newsletter_data_to_raw",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --target newsletter --start-date {{ params.start_date }} --end-date {{ params.end_date }}",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    clean_table_jobs = {}

    for name, params in clean_tables.items():
        task = bigquery_job_task(dag=dag, table=name, job_params=params)
        clean_table_jobs[name] = {
            "operator": task,
        }

    end_clean = DummyOperator(task_id="end_clean", dag=dag)

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

    end = DummyOperator(task_id="end", dag=dag)
    analytics_table_tasks = depends_loop(
        analytics_tables,
        analytics_table_jobs,
        end_clean,
        dag=dag,
        default_end_operator=end,
    )

    (
        gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> import_newsletter_data_to_raw
        >> import_transactional_data_to_tmp
        >> gce_instance_stop
        >> raw_table_tasks
        >> end_raw
        >> clean_table_tasks
        >> end_clean
        >> analytics_table_tasks
    )
