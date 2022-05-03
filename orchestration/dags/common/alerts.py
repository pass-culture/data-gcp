from urllib.parse import quote

from airflow import configuration
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from datetime import datetime
from dependencies.access_gcp_secrets import access_secret_data
from dependencies.config import GCP_PROJECT, ENV_SHORT_NAME

SLACK_CONN_ID = "slack_analytics"

ENV_EMOJI = {
    "prod": ":volcano: *PROD* :volcano:",
    "stg": ":fire: *STAGING* :fire:",
    "dev": ":snowflake: *DEV* :snowflake:",
}

JOB_TYPE = {
    "analytics": access_secret_data(
        GCP_PROJECT,
        "slack-composer-analytics-webhook-token",
        version_id=2,
        default=None,
    ),
    "prod": access_secret_data(
        GCP_PROJECT, "slack-composer-prod-webhook-token", version_id=2, default=None
    ),
    "stg": access_secret_data(
        GCP_PROJECT, "slack-composer-ehp-webhook-token", version_id=2, default=None
    ),
    "dev": access_secret_data(
        GCP_PROJECT, "slack-composer-ehp-webhook-token", version_id=2, default=None
    ),
}


def task_fail_slack_alert(context):
    return __task_fail_slack_alert(context, job_type=GCP_PROJECT)


def analytics_fail_slack_alert(context):
    return __task_fail_slack_alert(context, job_type="analytics")


def __task_fail_slack_alert(context, job_type):
    run_id = context.get("templates_dict", {}).get("run_id", "")
    status_str = "_(scheduled)_" if run_id.startswith("scheduled__") else "_(manual)_"
    webhook_token = JOB_TYPE.get(job_type)

    dag_url = "{base_url}/admin/airflow/graph?dag_id={dag_id}&execution_date={exec_date}".format(
        base_url=configuration.get("webserver", "BASE_URL"),
        dag_id=context["dag"].dag_id,
        exec_date=quote(context.get("execution_date").isoformat()),
    )
    dag_name = context.get("dag").dag_id
    task_name = context.get("task_instance").task_id
    task_url = context.get("task_instance").log_url
    execution_date = datetime.strftime(
        context.get("execution_date"), "%Y-%m-%d %H:%M:%S"
    )

    slack_msg = f"""
            {ENV_EMOJI[ENV_SHORT_NAME]}: 
            *Task* <{task_url}|{task_name}> has failed!
            *Dag*: <{dag_url}|{dag_name}>
            *Execution Time*: {execution_date} {status_str}
            """

    failed_alert = SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)
