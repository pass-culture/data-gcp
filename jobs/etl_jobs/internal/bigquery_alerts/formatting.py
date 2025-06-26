import datetime
import humanize
from typing import List
from core import TableInfo


def format_slack_message(
    tables: List[TableInfo],
    env_emoji: str,
    archive_days_threshold: int,
    delete_instead_of_archive: bool,
) -> str:
    """
    Format the Slack message for table alerts.

    Args:
        tables: List of tables that need attention
        env_emoji: Emoji to use based on environment
        archive_days_threshold: Number of days before a table is archived/deleted
        delete_instead_of_archive: Whether tables will be deleted instead of archived

    Returns:
        Formatted Slack message
    """
    if not tables:
        return "✅ All bigquery tables are updated according to schedule"

    message = f"""{env_emoji}
*:open_file_folder: Bigquery expected schedule alerts*
\n*Here is the list of tables that don't meet the expected update schedule:*
"""
    for table in tables:
        time_ago = humanize.naturaltime(
            datetime.datetime.now() - table.last_modified_time
        )
        alert_info = ""
        if table.days_since_update >= archive_days_threshold:
            alert_info = (
                "deleted today" if delete_instead_of_archive else "archived today"
            )
        else:
            days_left = archive_days_threshold - table.days_since_update
            action = "deleted" if delete_instead_of_archive else "archived"
            alert_info = f"will be {action} in {days_left} days"

        message += f"\n- {table.full_name} (last updated {time_ago}, {alert_info})"

    return message
