from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common import macros
from common.operators.biquery import bigquery_job_task
from dependencies.ml.clusterisation.import_data import (
    IMPORT_ITEM_EMBEDDINGS,
)
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
)

from common.alerts import task_fail_slack_alert
from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"

BASE_DIR = "data-gcp/jobs/ml_jobs/clusterisation"
DATE = "{{ yyyymmdd(ds) }}"

default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

CLUSTERING_CONFIG = [
    {
        "name": "default",
        "cluster_config_file_name": "default-config",
        "cluster_prefix": "default",
    },
    {
        "name": "unconstrained",
        "cluster_config_file_name": "unconstrained-config",
        "cluster_prefix": "unconstrained",
    },
]


with DAG(
    "clusterisation_item",
    default_args=default_args,
    description="Cluster offers from metadata embeddings",
    schedule_interval=get_airflow_schedule("0 0 * * 0"),  # every sunday
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-64",
            type="string",
        ),
    },
) as dag:
    bq_import_item_embeddings_task = bigquery_job_task(
        dag,
        f"bq_import_item_embeddings",
        IMPORT_ITEM_EMBEDDINGS,
        extra_params={},
    )

    for cluster_config in CLUSTERING_CONFIG:
        job_name = cluster_config["name"]
        cluster_config_file_name = cluster_config["cluster_config_file_name"]
        cluster_prefix = cluster_config["cluster_prefix"]
        gce_instance = f"clusterisation-{job_name}-{ENV_SHORT_NAME}"

        start = DummyOperator(task_id=f"start_{job_name}", dag=dag)
        end = DummyOperator(task_id=f"end_{job_name}", dag=dag)

        gce_instance_start = StartGCEOperator(
            task_id=f"gce_start_{job_name}_task",
            instance_name=gce_instance,
            preemptible=False,
            instance_type="{{ params.instance_type }}",
            retries=2,
            labels={"job_type": "long_ml"},
        )

        fetch_code = CloneRepositoryGCEOperator(
            task_id=f"fetch_{job_name}_code",
            instance_name=gce_instance,
            python_version="3.10",
            command="{{ params.branch }}",
            retries=2,
        )

        install_dependencies = SSHGCEOperator(
            task_id=f"install_{job_name}_dependencies",
            instance_name=gce_instance,
            base_dir=BASE_DIR,
            command="""pip install -r requirements.txt --user""",
        )

        preprocess_clustering = SSHGCEOperator(
            task_id=f"preprocess_{job_name}_clustering",
            instance_name=gce_instance,
            base_dir=BASE_DIR,
            command="PYTHONPATH=. python cluster/preprocess.py "
            f"--input-table {DATE}_import_item_embeddings "
            f"--output-table {DATE}_{cluster_prefix}_import_item_clusters_preprocess "
            f"--config-file-name {cluster_config_file_name} ",
        )

        generate_clustering = SSHGCEOperator(
            task_id=f"generate_{job_name}_clustering",
            instance_name=gce_instance,
            base_dir=BASE_DIR,
            command="PYTHONPATH=. python cluster/generate.py "
            f"--input-table {DATE}_{cluster_prefix}_import_item_clusters_preprocess "
            f"--output-table {cluster_prefix}_item_clusters "
            f"--config-file-name {cluster_config_file_name} ",
        )

        gce_instance_stop = StopGCEOperator(
            task_id=f"gce_{job_name}_stop_task", instance_name=gce_instance
        )

        (
            bq_import_item_embeddings_task
            >> start
            >> gce_instance_start
            >> fetch_code
            >> install_dependencies
            >> preprocess_clustering
            >> generate_clustering
            >> gce_instance_stop
            >> end
        )
