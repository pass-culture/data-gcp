from datetime import datetime

from common.alerts import SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN
from common.config import (
    DAG_TAGS,
    ENV_SHORT_NAME,
    get_airflow_uri,
    get_env_emoji,
)
from common.hooks.slack import SlackHook, SlackTeam

SEVERITY_TYPE_EMOJI = {
    "warn": ":warning:",
    "error": ":firecracker:",
}


def _get_owner_slack_message(dag_tags):
    """
    Get the slack message for the owner of the dag based on the environment and the tags.
    If the environment is prod, we ping the team, otherwise we just return the team name.
    """
    ping = True if ENV_SHORT_NAME == "prod" else False
    if DAG_TAGS.DS.value in dag_tags:
        return SlackTeam.DS.get_slack_message(ping=ping)
    else:
        return SlackTeam.DE.get_slack_message(ping=ping)


def task_fail_slack_alert(context):
    run_id = context["dag_run"].run_id
    is_scheduled = run_id.startswith("scheduled__")

    # alerts only for scheduled task.
    if is_scheduled:
        base_url = get_airflow_uri()
        last_task = context.get("task_instance")
        dag_id = context.get("dag").dag_id
        task_id = last_task.task_id

        dag_url = f"{base_url}/dags/{dag_id}/grid"
        task_url = (
            f"{dag_url}?dag_run_id={run_id}&task_id={task_id}&map_index=-1&tab=logs"
        )

        execution_date = datetime.strftime(
            context.get("execution_date"), "%Y-%m-%d %H:%M:%S"
        )

        owner_slack_message = _get_owner_slack_message(context.get("dag").tags)

        slack_msg = f"""
                {get_env_emoji()}:
                *Task* <{task_url}|{task_id}> has failed!
                *Dag*: <{dag_url}|{dag_id}>
                *Execution Time*: {execution_date}
                *Owner*: {owner_slack_message}
                """

        slack_hook = SlackHook(SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN)
        slack_hook.send_message(slack_msg)

    return None
