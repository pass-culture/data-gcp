from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dependencies.restore_to_big_query import restore_to_big_query


default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "restore_big_query",
    default_args=default_args,
    description="Restore csv dumps to big query",
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=20),
)


t1 = PythonOperator(task_id="restore", python_callable=restore_to_big_query, dag=dag)
