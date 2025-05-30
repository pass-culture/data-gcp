import typer
from core import BigQueryAlerts, AlertConfig
from utils import SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN, GCP_PROJECT_ID, ENV_SHORT_NAME


def run(
    archive_days: int = typer.Option(
        14, help="Number of days before archiving a table"
    ),
    archive_dataset: str = typer.Option(None, help="Dataset to archive tables to"),
    delete_instead_of_archive: bool = typer.Option(
        False, help="Delete tables instead of archiving them"
    ),
    skip_slack_alerts: bool = typer.Option(False, help="Skip sending Slack alerts"),
):
    try:
        config = AlertConfig(
            project_id=GCP_PROJECT_ID,
            env_short_name=ENV_SHORT_NAME,
            webhook_url=SLACK_DBT_TEST_CHANNEL_WEBHOOK_TOKEN,
            archive_days_threshold=archive_days,
            archive_dataset=archive_dataset,
            delete_instead_of_archive=delete_instead_of_archive,
            skip_slack_alerts=skip_slack_alerts,
        )
    except ValueError as e:
        typer.echo(f"Error in configuration: {str(e)}", err=True)
        raise typer.Exit(1)

    alerts = BigQueryAlerts(config)
    warning_tables = alerts.get_warning_tables()

    if not skip_slack_alerts:
        alerts.send_slack_alert(warning_tables)

    return


if __name__ == "__main__":
    typer.run(run)
