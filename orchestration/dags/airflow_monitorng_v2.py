import datetime
import json

from common.config import (
    SLACK_CONN_PASSWORD,
)
from sqlalchemy import func

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session

# # Slack webhook URL (store in Airflow Variables or Connections for security)
# SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/your/webhook/url"


def generate_dag_status_report():
    dag_statuses = {"complete": 0, "running": 0, "error": 0, "dags_in_error": []}

    with create_session() as session:
        # Query the last DAG runs and their states
        # Subquery to get the latest run for each dag_id
        subquery = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("latest_execution_date"),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        # Join the subquery to get the states of the latest runs
        latest_runs = (
            session.query(DagRun.dag_id, DagRun.state)
            .join(
                subquery,
                (DagRun.dag_id == subquery.c.dag_id)
                & (DagRun.execution_date == subquery.c.latest_execution_date),
            )
            .all()
        )

        for dag_id, state in latest_runs:
            if state == "success":
                dag_statuses["complete"] += 1
            elif state == "running":
                dag_statuses["running"] += 1
            elif state in ["failed", "up_for_retry"]:
                dag_statuses["error"] += 1
                dag_statuses["dags_in_error"].append(dag_id)

    message = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*Cluster Summary for {datetime.date.today()}*:\n"
                        f"- {dag_statuses['complete']} DAGs complete\n"
                        f"- {dag_statuses['running']} DAGs running\n"
                        f"- {dag_statuses['error']} DAGs in error\n"
                        f"- Dags in error: {', '.join(dag_statuses['dags_in_error'])}"
                    ),
                },
            }
        ]
    }
    return json.dumps(message)


# def format_slack_message(**context):
#     statuses = context["ti"].xcom_pull(task_ids="generate_report")
#     message = (
#         f"*Cluster Summary for {datetime.date.today()}*:\n"
#         f"- {statuses['complete']} DAGs complete\n"
#         f"- {statuses['running']} DAGs running\n"
#         f"- {statuses['error']} DAGs in error\n"
#         f"- Dags in error: {', '.join(statuses['dags_in_error'])}"
#     )
#     return message


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
    tags=["DEBUG_ON_DEV", "@Laurent"],
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

    send_slack = HttpOperator(
        task_id="send_slack_notif_success",
        method="POST",
        http_conn_id="http_slack_default",
        endpoint=f"{SLACK_CONN_PASSWORD}",
        data="{{ task_instance.xcom_pull(task_ids='generate_report') }}",
        headers={"Content-Type": "application/json"},
    )

    generate_report >> send_slack
