import os
from urllib.parse import quote

from airflow import AirflowException, configuration, settings
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models.connection import Connection

from dependencies.access_gcp_secrets import access_secret_data
from dependencies.config import GCP_PROJECT, ENV_SHORT_NAME

SLACK_CONN_ID = "slack_analytics"
SLACK_CONN_PASSWORD = access_secret_data(GCP_PROJECT, "slack-analytics-conn-password")

if ENV_SHORT_NAME == "prod":
    ENV_WITH_STYLE = ":volcan: *PROD* :volcan:"
if ENV_SHORT_NAME == "stg":
    ENV_WITH_STYLE = ":fire: *STAGING* :fire:"
else:
    ENV_WITH_STYLE = ":flocon_de_neige: *DEV* :flocon_de_neige:"

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
            :red_circle: IMPORT TO ANALYTICS FAILED.
            *Environnement*: {env}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *DAG Url*: {dag_url}
            """.format(
        env=ENV_WITH_STYLE,
        dag=context.get("dag").dag_id,
        exec_date=context.get("execution_date"),
        dag_url=link,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=SLACK_CONN_PASSWORD,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
