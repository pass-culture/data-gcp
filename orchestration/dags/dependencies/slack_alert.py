import os
from urllib.parse import quote

from airflow import AirflowException, configuration, settings
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models.connection import Connection

from dependencies.access_gcp_secrets import access_secret_data

SLACK_CONN_ID = "slack"
GCP_PROJECT = os.environ.get("GCP_PROJECT")
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT, "slack-conn-password")

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
    link = "{base_url}/admin/airflow/graph?dag_id={dag_id}&execution_date={exec_date}".format(
        base_url=configuration.get("webserver", "BASE_URL"),
        dag_id=context["dag"].dag_id,
        exec_date=quote(context.get("execution_date").isoformat()),
    )

    slack_msg = """
            :red_circle: Dag Failed.
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *DAG Url*: {dag_url}
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        dag_url=link,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        message=slack_msg,
        username="Cloud Composer",
    )

    return failed_alert.execute(context=context)
