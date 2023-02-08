from urllib.parse import quote

from airflow import configuration
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from datetime import datetime
from common.access_gcp_secrets import access_secret_data
from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME, SLACK_CONN_ID


ENV_EMOJI = {
    "prod": ":volcano: *PROD* :volcano:",
    "stg": ":fire: *STAGING* :fire:",
    "dev": ":snowflake: *DEV* :snowflake:",
}

JOB_TYPE = {
    "analytics": access_secret_data(
        GCP_PROJECT_ID,
        "slack-composer-analytics-webhook-token",
        default=None,
    ),
    "prod": access_secret_data(
        GCP_PROJECT_ID, "slack-composer-prod-webhook-token", default=None
    ),
    "stg": access_secret_data(
        GCP_PROJECT_ID, "slack-composer-ehp-webhook-token", default=None
    ),
    "dev": access_secret_data(
        GCP_PROJECT_ID, "slack-composer-ehp-webhook-token", default=None
    ),
}


def task_fail_slack_alert(context):
    return __task_fail_slack_alert(context, job_type=ENV_SHORT_NAME)


def analytics_fail_slack_alert(context):
    return __task_fail_slack_alert(context, job_type="analytics")


def __task_fail_slack_alert(context, job_type):
    run_id = context["dag_run"].run_id
    is_scheduled = run_id.startswith("scheduled__")
    # alerts only for scheduled task or prod.
    if is_scheduled or ENV_SHORT_NAME == "prod":
        webhook_token = JOB_TYPE.get(job_type)
        dag_url = (
            "{base_url}/graph?dag_id={dag_id}&root=&execution_date={exec_date}".format(
                base_url=configuration.get("webserver", "BASE_URL"),
                dag_id=context["dag"].dag_id,
                exec_date=quote(context.get("execution_date").isoformat()),
            )
        )
        last_task = context.get("task_instance")
        dag_name = context.get("dag").dag_id
        task_name = last_task.task_id
        task_url = last_task.log_url
        execution_date = datetime.strftime(
            context.get("execution_date"), "%Y-%m-%d %H:%M:%S"
        )

        slack_msg = f"""
                {ENV_EMOJI[ENV_SHORT_NAME]}: 
                *Task* <{task_url}|{task_name}> has failed!
                *Dag*: <{dag_url}|{dag_name}>
                *Execution Time*: {execution_date}
                """

        failed_alert = SlackWebhookOperator(
            task_id="slack_alert",
            http_conn_id=SLACK_CONN_ID,
            webhook_token=webhook_token,
            message=slack_msg,
            username="airflow",
        )
        return failed_alert.execute(context=context)
    return None
