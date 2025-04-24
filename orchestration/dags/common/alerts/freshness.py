import ast

from common.access_gcp_secrets import access_secret_data
from common.config import (
    GCP_PROJECT_ID,
    get_env_emoji,
)
from common.hooks.slack import SlackHook

SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN = access_secret_data(
    GCP_PROJECT_ID,
    "slack-composer-dbt-test-webhook-token",
    default=None,
)


def bigquery_freshness_alert(warning_table_list, **context):
    warning_tables = ast.literal_eval(warning_table_list)
    if not isinstance(warning_tables, list):
        raise ValueError("warning_table_list should be a list.")

    if len(warning_tables) > 0:
        slack_msg = f"""{get_env_emoji()}
        *:open_file_folder: Bigquery expected schedule alerts *
        \n *Here is the list of tables that don't meet the expected update schedule :*
        """
        for table in warning_tables:
            slack_msg += f"\n- {table}"

    else:
        slack_msg = "✅ All bigquery tables are updated according to schedule"

    slack_hook = SlackHook(SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN)
    slack_hook.send_message(slack_msg)

    return None
