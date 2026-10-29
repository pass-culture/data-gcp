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
        return f"• `{table}`"

    owner = dbt_owners.get(table["table"], "not in dbt")
    writer = (table.get("last_writer") or "unknown").split("@")[0]
    details = [f"dbt: {owner}", f"last writer: {writer}"]
    if table.get("sandbox"):
        details.append(f":package: {table['sandbox']}")
    return (
        f"• `{table['table']}` — *{table['days_since_update']}d* stale "
        f"(expected {table['schedule']}, last update {table['last_modified']})\n"
        f"      _{' · '.join(details)}_"
    )


def bigquery_freshness_alert(warning_table_list, **context):
    warning_tables = ast.literal_eval(warning_table_list)
    if not isinstance(warning_tables, list):
        raise ValueError("warning_table_list should be a list.")

    if len(warning_tables) > 0:
        dbt_owners = get_dbt_owners()
        # Most stale first; older job versions sent table names only.
        warning_tables = sorted(
            warning_tables,
            key=lambda table: (
                -table["days_since_update"] if isinstance(table, dict) else 0
            ),
        )
        lines = [
            get_env_emoji(),
            f":open_file_folder: *BigQuery freshness alert* — "
            f"{len(warning_tables)} table(s) behind their expected update schedule",
            "",
        ]
        lines += [format_warning_table(table, dbt_owners) for table in warning_tables]
        slack_msg = "\n".join(lines)

    else:
        slack_msg = "✅ All bigquery tables are updated according to schedule"

    slack_hook = SlackHook(SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN)
    slack_hook.send_message(slack_msg)

    return None
