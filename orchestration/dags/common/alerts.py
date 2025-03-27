import ast
import json
import re
from datetime import datetime

import requests
from common.access_gcp_secrets import access_secret_data
from common.config import (
    AIRFLOW_URI,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    LOCAL_ENV,
)
from common.operators.gce import StopGCEOperator

from airflow import configuration

HTTP_HOOK = "https://hooks.slack.com/services/"
DEFAULT_HEADERS = {"Content-Type": "application/json"}
ENV_EMOJI = {
    "prod": ":volcano: *PROD* :volcano:",
    "stg": ":fire: *STAGING* :fire:",
    "dev": ":snowflake: *DEV* :snowflake:",
}

SEVERITY_TYPE_EMOJI = {
    "warn": ":warning:",
    "error": ":firecracker:",
}

JOB_TYPE = {
    "analytics": access_secret_data(
        GCP_PROJECT_ID,
        "slack-composer-analytics-webhook-token",
        default=None,
    ),
    "dbt-test": access_secret_data(
        GCP_PROJECT_ID,
        "slack-composer-dbt-test-webhook-token",
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

GROUP_SLACK_IDS = {DAG_TAGS.DS.value: "S08CVKQ4K9S", DAG_TAGS.DE.value: "S08CT44F7J6"}


def remove_duplicate_prefix(instance_name: str, prefix: str) -> str:
    # Build a regex pattern that matches one or more occurrences of the prefix followed by a hyphen at the beginning
    pattern = rf"^(?:{re.escape(prefix + '-')})+"
    match = re.match(pattern, instance_name)
    if match:
        # Count how many occurrences of the prefix (with hyphen) are found
        occurrences = match.group(0).count(prefix + "-")
        # Only modify if there is more than one occurrence
        if occurrences > 1:
            # Replace the matched sequence with a single occurrence of prefix and hyphen
            instance_name = re.sub(pattern, f"{prefix}-", instance_name)
    return instance_name


def task_fail_slack_alert(context):
    return __task_fail_slack_alert(context, job_type=ENV_SHORT_NAME)


def analytics_fail_slack_alert(context):
    return __task_fail_slack_alert(context, job_type="analytics")


def on_failure_callback_stop_vm(context: dict):
    """
    Callback to stop the VM associated with the failing task,
    using the custom StopGCEOperator with a proper TaskInstance context.
    """
    failing_task = context.get("task")
    if hasattr(failing_task, "instance_name"):
        # Ensure any templated fields are rendered
        failing_task.render_template_fields(context)
        instance_name = failing_task.instance_name
        # instance_name = remove_duplicate_prefix(instance_name, GCE_BASE_PREFIX)

        # Retrieve the TaskInstance from context
        ti = context.get("ti")
        if not ti:
            raise ValueError("TaskInstance not found in context.")

        # Instantiate the StopGCEOperator (note that it adds a prefix internally)
        stop_vm_operator = StopGCEOperator(
            task_id="stop_vm_on_failure_callback",
            instance_name=instance_name,
        )
        # Run the operator using the run() method so that it receives the proper context
        stop_vm_operator.run(
            start_date=ti.start_date,
            end_date=ti.end_date,
            ignore_ti_state=True,
        )


def on_failure_combined_callback(context):
    task_fail_slack_alert(context)
    on_failure_callback_stop_vm(context)


def get_env_emoji():
    base_url = configuration.get("webserver", "BASE_URL")
    base_emoji = ENV_EMOJI[ENV_SHORT_NAME]
    if LOCAL_ENV:
        return ENV_EMOJI["local"]
    if "localhost" in base_url:
        return f"{base_emoji} (k8s)"
    return base_emoji


def get_airflow_uri():
    base_url = configuration.get("webserver", "BASE_URL")
    if LOCAL_ENV:
        return base_url
    if "localhost" in base_url:
        return f"https://{AIRFLOW_URI}"
    return base_url


def __task_fail_slack_alert(context, job_type):
    run_id = context["dag_run"].run_id
    is_scheduled = run_id.startswith("scheduled__")

    # alerts only for scheduled task.
    if is_scheduled:
        base_url = get_airflow_uri()
        webhook_token = JOB_TYPE.get(job_type)

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

        dag_tags = context.get("dag").tags
        if DAG_TAGS.DS.value in dag_tags:
            owner_tag = DAG_TAGS.DS.value
        else:
            owner_tag = DAG_TAGS.DE.value

        owner_slack_id = GROUP_SLACK_IDS.get(owner_tag, None)

        slack_msg = f"""
                {get_env_emoji()}:
                *Task* <{task_url}|{task_id}> has failed!
                *Dag*: <{dag_url}|{dag_id}>
                *Execution Time*: {execution_date}
                *Owner*: <!subteam^{owner_slack_id}>
                """

        response = requests.post(
            f"{HTTP_HOOK}{webhook_token}",
            data=json.dumps({"text": f"{slack_msg}"}),
            headers=DEFAULT_HEADERS,
        )
        if response.status_code != 200:
            raise ValueError(
                f"Request to Slack returned an error {response.status_code}, response: {response.text}"
            )

    return None


def dbt_test_slack_alert(results_json, manifest_json, job_type="dbt-test", **context):
    webhook_token = JOB_TYPE.get(job_type)
    slack_header = f"""{ENV_EMOJI[ENV_SHORT_NAME]}
    *:page_facing_up: DBT tests report :page_facing_up:*
    """
    if isinstance(results_json, str):
        results_json = ast.literal_eval(results_json)

    if isinstance(manifest_json, str):
        manifest_json = ast.literal_eval(manifest_json)

    tests_manifest = {
        node: values
        for node, values in manifest_json["nodes"].items()
        if values["resource_type"] == "test"
    }
    if "results" in results_json:
        if results_json["results"] == []:
            slack_msg = slack_header
            slack_msg += "\n:zero: No detected tests"
        else:
            tests_results = results_json["results"]
            slack_msg = slack_header
            test_nodes = {}
            for result in tests_results:
                node = result["unique_id"]
                if result["status"] != "pass":
                    if test_nodes.get(result["unique_id"]) is None:
                        test_nodes[result["unique_id"]] = {
                            result["unique_id"]: [result["status"], result["message"]]
                        }
                    else:
                        test_nodes[result["unique_id"]] = {
                            **test_nodes[result["unique_id"]],
                            **{
                                result["unique_id"]: [
                                    result["status"],
                                    result["message"],
                                ]
                            },
                        }
            test_nodes = dict(
                sorted(
                    test_nodes.items(),
                    key=lambda item: manifest_json["nodes"][item[0]]["meta"].get(
                        "owner", "@team-data"
                    ),
                )
            )
            for node, tests_results in test_nodes.items():
                tested_node = tests_manifest[node]["attached_node"]
                slack_msg = "\n".join(
                    [
                        slack_msg,
                        f"""Ownership: {manifest_json["nodes"][node]["meta"].get("owner","@team-data")}""",
                        f"""Model {tested_node.split('.')[-1]} failed the following tests: """,
                    ]
                    + [
                        f"""{SEVERITY_TYPE_EMOJI[res[0]]} *Test:* {tests_manifest[test]["alias"]}"""
                        + f" has failed with severity {res[0]}\n"
                        + f">_{res[1]}_"
                        for test, res in tests_results.items()
                    ]
                )
    else:
        slack_msg = slack_header
        slack_msg += "\nNo tests have been run"

    if slack_msg == slack_header:
        slack_msg += "\nAll tests passed succesfully! :tada:"

    response = requests.post(
        f"{HTTP_HOOK}{webhook_token}",
        data=json.dumps({"text": f"{slack_msg}"}),
        headers=DEFAULT_HEADERS,
    )
    if response.status_code != 200:
        raise ValueError(
            f"Request to Slack returned an error {response.status_code}, response: {response.text}"
        )

    return None


def bigquery_freshness_alert(warning_table_list, job_type="dbt-test", **context):
    webhook_token = JOB_TYPE.get(job_type)
    warning_tables = ast.literal_eval(warning_table_list)
    if not isinstance(warning_tables, list):
        raise ValueError("warning_table_list should be a list.")

    if len(warning_tables) > 0:
        slack_msg = f"""{ENV_EMOJI[ENV_SHORT_NAME]}
        *:open_file_folder: Bigquery expected schedule alerts *
        \n *Here is the list of tables that don't meet the expected update schedule :*
        """
        for table in warning_tables:
            slack_msg += f"\n- {table}"

    else:
        slack_msg = "✅ All bigquery tables are updated according to schedule"

    response = requests.post(
        f"{HTTP_HOOK}{webhook_token}",
        data=json.dumps({"text": f"{slack_msg}"}),
        headers=DEFAULT_HEADERS,
    )
    if response.status_code != 200:
        raise ValueError(
            f"Request to Slack returned an error {response.status_code}, response: {response.text}"
        )
    return None
