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
    IMPORT_ITEM_CLUSTERS,
    IMPORT_ITEM_EMBEDDINGS,
)


from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, ENV_SHORT_NAME
from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"clusterisation-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/clusterisation"
DATE = "{{ yyyymmdd(ds) }}"

default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "clusterisation_item",
    default_args=default_args,
    description="Cluster offers from metadata embeddings",
    schedule_interval=get_airflow_schedule("0 0 * * 0"),  # every sunday
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
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-highmem-32",
            type="string",
        ),
        "config_file_name": Param(
            default="default-config",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

    bq_import_item_embeddings_task = bigquery_job_task(
        dag, "bq_import_item_embeddings", IMPORT_ITEM_EMBEDDINGS, extra_params={}
    )
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "long_ml"},
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        python_version="3.10",
        command="{{ params.branch }}",
        retries=2,
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="""pip install -r requirements.txt --user""",
    )

    preprocess_clustering = SSHGCEOperator(
        task_id="preprocess_clustering",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python cluster/preprocess.py "
        f"--input-table {DATE}_import_item_clusters "
        f"--output-table {DATE}_import_item_clusters_preprocesss "
        "--config-file-name {{ params.config_file_name }} ",
    )

    generate_clustering = SSHGCEOperator(
        task_id="generate_clustering",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python cluster/generate.py "
        f"--input-table {DATE}_import_item_clusters_preprocesss "
        f"--output-table item_clusters "
        "--config-file-name {{ params.config_file_name }} ",
    )

    bq_import_item_clusters_task = bigquery_job_task(
        dag, "bq_import_item_clusters", IMPORT_ITEM_CLUSTERS, extra_params={}
    )

    generate_topics = SSHGCEOperator(
        task_id="generate_topics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python topics/generate.py "
        f"--input-table item_clusters "
        f"--item-topics-labels-output-table item_topics_labels "
        f"--item-topics-output-table item_topics ",
    )

    clean_topics = SSHGCEOperator(
        task_id="generate_topics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python topics/clean.py "
        f"--input-table item_clusters "
        f"--item-topics-labels-output-table item_topics_labels "
        f"--item-topics-output-table item_topics ",
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> bq_import_item_embeddings_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess_clustering
        >> preprocess_clustering
        >> bq_import_item_clusters_task
        >> generate_topics
        >> clean_topics
        >> gce_instance_stop
        >> end
    )
