import ast

from common.access_gcp_secrets import access_secret_data
from common.config import (
    GCP_PROJECT_ID,
    PATH_TO_DBT_TARGET,
    get_env_emoji,
)
from common.dbt.dag_utils import load_manifest
from common.hooks.slack import SlackHook

SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN = access_secret_data(
    GCP_PROJECT_ID,
    "slack-composer-dbt-test-webhook-token",
    default=None,
)


def get_dbt_owners():
    """Map "dataset.table" to the dbt meta owner of the model building it."""
    manifest = load_manifest(PATH_TO_DBT_TARGET)
    return {
        f"{node['schema']}.{node['alias']}": node["config"]
        .get("meta", {})
        .get("owner", "no owner")
        for node in manifest["nodes"].values()
        if node.get("resource_type") in ("model", "snapshot", "seed")
    }


def format_warning_table(table, dbt_owners):
    # Older job versions sent table names only.
    if isinstance(table, str):
        return f"\n- {table}"

    owner = dbt_owners.get(table["table"], "not in dbt")
    writer = (table.get("last_writer") or "unknown").split("@")[0]
    sandbox = f" · {table['sandbox']}" if table.get("sandbox") else ""
    return (
        f"\n- {table['table']} — last update {table['last_modified']} "
        f"({table['days_since_update']}d, expected {table['schedule']}) "
        f"· dbt: {owner} · last writer: {writer}{sandbox}"
    )


def bigquery_freshness_alert(warning_table_list, **context):
    warning_tables = ast.literal_eval(warning_table_list)
    if not isinstance(warning_tables, list):
        raise ValueError("warning_table_list should be a list.")

    if len(warning_tables) > 0:
        dbt_owners = get_dbt_owners()
        slack_msg = f"""{get_env_emoji()}
        *:open_file_folder: Bigquery expected schedule alerts *
        \n *Here is the list of tables that don't meet the expected update schedule :*
        """
        for table in warning_tables:
            slack_msg += format_warning_table(table, dbt_owners)

    else:
        slack_msg = "✅ All bigquery tables are updated according to schedule"

    slack_hook = SlackHook(SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN)
    slack_hook.send_message(slack_msg)

    return None
