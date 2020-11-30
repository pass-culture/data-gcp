import os
from urllib.parse import quote

from airflow import settings, AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.connection import Connection


SLACK_CONN_ID = "slack"
SLACK_CONN_PASSWORD = os.environ.get("SLACK_CONN_PASSWORD")
AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL")

try:
    conn = BaseHook.get_connection(SLACK_CONN_ID)
except AirflowException:
    conn = Connection(
        conn_id=SLACK_CONN_ID,
        conn_type="http",
        host="https://hooks.slack.com/services",
        password=SLACK_CONN_PASSWORD,
    )

    session = settings.Session()
    session.add(conn)
    session.commit()


def task_fail_slack_alert(context):
    def log_url():
        iso = quote(context.get("execution_date").isoformat())
        return "{airflow_base_url}/admin/airflow/log?execution_date={iso}&task_id={task_id}&dag_id={dag_id}".format(
            airflow_base_url=AIRFLOW_BASE_URL,
            iso=iso,
            task_id=context.get("task_instance").task_id,
            dag_id=context.get("task_instance").dag_id,
        )

    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=log_url(),
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
