from datetime import datetime, timedelta

from common import macros
from common.alerts import on_failure_combined_callback
from common.config import (
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
)
from common.operators.bigquery import bigquery_job_task
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from dependencies.ml.offline_recommendation.export_to_backend import (
    params as params_export,
)
from dependencies.ml.offline_recommendation.import_users import params

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"offline-recommendation-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/ml_jobs/offline_recommendation"
DATE = "{{ yyyymmdd(ds) }}"
STORAGE_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/offline_recommendation_{ENV_SHORT_NAME}/offline_recommendation_{DATE}"
DAG_NAME = "offline_recommendation"

default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": on_failure_combined_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "TOKENIZERS_PARALLELISM": "false",
    "API_TOKEN_SECRET_ID": f"api-reco-token-{ENV_SHORT_NAME}",
}
with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Produce offline recommendation",
    schedule_interval=get_airflow_schedule("0 0 * * 0"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    import_data_tasks = []
    get_offline_predictions = []
    for query_params in params:
        import_data_tasks.append(
            bigquery_job_task(
                dag,
                f"""import_{query_params["table"]}""",
                query_params,
                extra_params={},
            )
        )

        get_offline_predictions.append(
            SSHGCEOperator(
                task_id=f"""get_offline_predictions_{query_params["table"]}""",
                instance_name=GCE_INSTANCE,
                base_dir=BASE_PATH,
                environment=dag_config,
                command="PYTHONPATH=. python main.py "
                f"""--input-table {query_params["destination_table"]} --output-table offline_recommendation_{query_params["destination_table"]}""",
            )
        )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        python_version="3.10",
        retries=2,
    )

    export_to_backend_tasks = []
    for query_params in params_export:
        export_to_backend_tasks.append(
            bigquery_job_task(
                dag,
                f"""export_to_backend_{query_params["table"]}""",
                query_params,
                extra_params={},
            )
        )

    end = DummyOperator(task_id="end", dag=dag)
    (
        start
        >> import_data_tasks
        >> gce_instance_start
        >> fetch_install_code
        >> get_offline_predictions[0]
        >> get_offline_predictions[1]
        >> export_to_backend_tasks
        >> end
    )
