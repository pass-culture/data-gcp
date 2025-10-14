from datetime import datetime, timedelta

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_REGION = "europe-west1"

BASE_PATH = "data-gcp/jobs/ml_jobs/clusterisation"
DATE = "{{ yyyymmdd(ds) }}"
DAG_NAME = "embedding_clusterisation_item"

default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": on_failure_vm_callback,
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
    DAG_NAME,
    default_args=default_args,
    description="Cluster items from metadata embeddings",
    schedule_interval=get_airflow_schedule("0 21 * * 0"),  # every sunday
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
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
            labels={"job_type": "long_ml", "dag_name": DAG_NAME},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id=f"fetch_install_code_{job_name}",
            instance_name=gce_instance,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=BASE_PATH,
        )

        preprocess_clustering = SSHGCEOperator(
            task_id=f"preprocess_{job_name}_clustering",
            instance_name=gce_instance,
            base_dir=BASE_PATH,
            command="PYTHONPATH=. python cluster/preprocess.py "
            f"--input-dataset-name ml_input_{ENV_SHORT_NAME} "
            f"--input-table-name item_embedding_clusterisation "
            f"--output-dataset-name tmp_{ENV_SHORT_NAME} "
            f"--output-table-name {DATE}_{cluster_prefix}_import_item_clusters_preprocess "
            f"--config-file-name {cluster_config_file_name} ",
            deferrable=True,
        )

        generate_clustering = SSHGCEOperator(
            task_id=f"generate_{job_name}_clustering",
            instance_name=gce_instance,
            base_dir=BASE_PATH,
            command="PYTHONPATH=. python cluster/generate.py "
            f"--input-dataset-name tmp_{ENV_SHORT_NAME} "
            f"--input-table-name {DATE}_{cluster_prefix}_import_item_clusters_preprocess "
            f"--output-dataset-name ml_preproc_{ENV_SHORT_NAME} "
            f"--output-table-name {cluster_prefix}_item_cluster "
            f"--config-file-name {cluster_config_file_name} ",
            deferrable=True,
        )

        gce_instance_stop = DeleteGCEOperator(
            task_id=f"gce_{job_name}_stop_task", instance_name=gce_instance
        )

        (
            start
            >> gce_instance_start
            >> fetch_install_code
            >> preprocess_clustering
            >> generate_clustering
            >> gce_instance_stop
            >> end
        )
