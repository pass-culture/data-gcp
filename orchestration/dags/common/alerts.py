from datetime import datetime
from urllib.parse import quote

from airflow import configuration
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from common.access_gcp_secrets import access_secret_data
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, SLACK_CONN_ID

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
    # alerts only for scheduled task.
    if is_scheduled:
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


def dbt_test_fail_slack_alert(
    test_output_dict, fail_type, dag, job_type=ENV_SHORT_NAME, **context
):
    webhook_token = JOB_TYPE.get(job_type)

    slack_header = f"""{ENV_EMOJI[ENV_SHORT_NAME]}
    DBT {fail_type} tests report:
    """
    slack_msg = slack_header
    for node, failed_tests in test_output_dict.items():
        if len(failed_tests) > 0:
            slack_msg = (
                "\n".join([slack_msg, f"""*model* <{node}>"""])
                + " has failed following tests with message:\n"
            )
            for testdict in failed_tests:
                if len(testdict) > 0:
                    for test, message in testdict.items():
                        slack_msg = "\n\t".join(
                            [
                                slack_msg,
                                f"""*Test* <{test}>""" + "\n\t" + f"""{message}>""",
                            ]
                        )
    if slack_msg == slack_header:
        slack_msg += "\nAll tests passed succesfully! :tada:"

    dbt_test_fail_slack_alert_alert = SlackWebhookOperator(
        task_id=f"""slack_alert_{fail_type}""",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=webhook_token,
        message=slack_msg,
        username="airflow",
        dag=dag,
    )
    return dbt_test_fail_slack_alert_alert.execute(context=context)
