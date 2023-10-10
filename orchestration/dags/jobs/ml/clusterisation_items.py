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
from common.operators.biquery import bigquery_job_task
from dependencies.ml.clusterisation.import_data import params
from dependencies.ml.clusterisation.postprocess import postprocess_params

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, DATA_GCS_BUCKET_NAME, ENV_SHORT_NAME

from common.utils import get_airflow_schedule

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"clusterisation-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/clusterisation"
DATE = "{{ yyyymmdd(ds) }}"
STORAGE_PATH = (
    f"gs://{DATA_GCS_BUCKET_NAME}/clusterisation_{ENV_SHORT_NAME}/clusterisation_{DATE}"
)
default_args = {
    "start_date": datetime(2023, 8, 2),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag_config = {
    "TOKENIZERS_PARALLELISM": "false",
}

categories_config = [
    {
        "group": "MANGA",
        "target_n_clusters": "30",
    },
    {
        "group": "LIVRES",
        "target_n_clusters": "150",
    },
    {
        "group": "CINEMA",
        "target_n_clusters": "150",
    },
    {
        "group": "MUSIQUE",
        "target_n_clusters": "150",
    },
    {
        "group": "ARTS",
        "target_n_clusters": "30",
    },
    {
        "group": "SORTIES",
        "target_n_clusters": "30",
    },
]
with DAG(
    "clusterisation_item",
    default_args=default_args,
    description="Cluster offers from metadata embeddings",
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
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-highmem-32",
            type="string",
        ),
        "config_file_name": Param(
            default="default-config",
            type="string",
        ),
        "target_n_clusters": Param(
            default="150",
            type="string",
        ),
    },
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)

    data_collect_task = bigquery_job_task(
        dag, "import_item_batch", params, extra_params={}
    )
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name=GCE_INSTANCE,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "ml"},
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

    preprocess = SSHGCEOperator(
        task_id="preprocess",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command="PYTHONPATH=. python preprocess.py "
        f"--input-table {DATE}_clusterisation_items_raw --output-table {DATE}_clustering_item_full_encoding_w_cat_group "
        "--config-file-name {{ params.config_file_name }} ",
    )

    clusterisation_task = []
    for category_config in categories_config:
        group = category_config["group"]
        output_table = (
            f"""{DATE}_item_clusters_{category_config["target_n_clusters"]}_{group}"""
        )
        clusterisation_task.append(
            SSHGCEOperator(
                task_id=f"clusterisation_{group}",
                instance_name=GCE_INSTANCE,
                base_dir=BASE_DIR,
                environment=dag_config,
                command="PYTHONPATH=. python clusterisation.py "
                f"""--target-n-clusters {category_config["target_n_clusters"]} """
                f"--input-table {DATE}_clustering_item_full_encoding_w_cat_group "
                f"--output-table {output_table} "
                f"--clustering-group {group} ",
            )
        )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    postprocess_clustering = bigquery_job_task(
        dag, "postprocess_clustering", postprocess_params, extra_params={}
    )

    (
        start
        >> data_collect_task
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> preprocess
    )
    (
        preprocess
        # for task in clusterisation_task:
        >> clusterisation_task[0]
        >> clusterisation_task[1]
        >> clusterisation_task[2]
        >> clusterisation_task[3]
        >> clusterisation_task[4]
        >> clusterisation_task[5]
        >> gce_instance_stop
    )
    (gce_instance_stop >> postprocess_clustering >> end)
