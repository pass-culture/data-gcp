import datetime

from airflow import DAG
from airflow.models import DagBag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# # Slack webhook URL (store in Airflow Variables or Connections for security)
# SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/your/webhook/url"


def generate_dag_status_report():
    dag_bag = DagBag()
    dag_statuses = {"complete": 0, "running": 0, "error": 0}

    for dag_id in dag_bag.dag_ids:
        dag = dag_bag.get_dag(dag_id)
        latest_run = dag.get_latest_execution_date()

        if latest_run:
            session = dag_bag.get_session()
            dag_run = dag.get_last_dagrun(session=session)

            if dag_run and dag_run.state:
                state = dag_run.state
                if state == "success":
                    dag_statuses["complete"] += 1
                elif state == "running":
                    dag_statuses["running"] += 1
                elif state in ["failed", "up_for_retry"]:
                    dag_statuses["error"] += 1

    return dag_statuses


def format_slack_message(**context):
    statuses = context["ti"].xcom_pull(task_ids="generate_report")
    message = (
        f"*Cluster Summary for {datetime.date.today()}*:\n"
        f"- {statuses['complete']} DAGs complete\n"
        f"- {statuses['running']} DAGs running\n"
        f"- {statuses['error']} DAGs in error"
    )
    return message


def simple_print_task(**context):
    # Retrieve the formatted Slack message from XCom
    message = context["ti"].xcom_pull(task_ids="format_slack_message")
    print(f"Slack Message: {message}")


with DAG(
    "airflow_monitoring_v2",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    description="Send daily DAG status report to Slack",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_dag_status_report,
    )

    # send_report = SlackWebhookOperator(
    #     task_id="send_slack_report",
    #     http_conn_id=None,  # Not needed for webhook URL
    #     webhook_token=SLACK_WEBHOOK_URL,
    #     message="{{ task_instance.xcom_pull(task_ids='generate_report') | format_slack_message }}",
    # )

    send_report = PythonOperator(
        task_id="simple_print",
        python_callable=simple_print_task,
        provide_context=True,
    )

    generate_report >> send_report
