import airflow
from airflow import DAG
from common.operators.gce import CleanGCEOperator
from datetime import timedelta
from common.config import ENV_SHORT_NAME
from common.utils import get_airflow_schedule

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


clean_default_delay = {"prod": 60 * 1, "dev": 60 * 2, "stg": 60 * 1}
clean_ml_delay = {"prod": 60 * 12, "dev": 60 * 3, "stg": 60 * 6}


dag = DAG(
    "airflow_cleaning",
    default_args=default_args,
    catchup=False,
    description="Automatic cleaning of VMs",
    schedule_interval=get_airflow_schedule("0 * * * *"),
    dagrun_timeout=timedelta(hours=1),
)

dag_cleaning = CleanGCEOperator(
    dag=dag,
    task_id="clean_default_vm_gce_operator",
    timeout_in_minutes=clean_default_delay[ENV_SHORT_NAME],
    job_type="default",
)
ml_cleaning = CleanGCEOperator(
    dag=dag,
    task_id="clean_ml_vm_gce_operator",
    timeout_in_minutes=clean_ml_delay[ENV_SHORT_NAME],
    job_type="ml",
)
