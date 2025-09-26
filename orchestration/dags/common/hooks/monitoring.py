from common.config import (
    DATA_GCS_BUCKET_NAME,
    ELEMENTARY_REPORT_URL,
    ENV_SHORT_NAME,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from elementary.config.config import Config
from elementary.monitor.data_monitoring.alerts.data_monitoring_alerts import (
    DataMonitoringAlerts,
)
from elementary.monitor.data_monitoring.report.data_monitoring_report import (
    DataMonitoringReport,
)
from elementary.monitor.data_monitoring.schema import FiltersSchema
from elementary.tracking.anonymous_tracking import AnonymousCommandLineTracking


class ElementaryReport:
    def generate_report(
        self, report_file_path: str, days_back: int = 7, target_path: str = None
    ) -> bool:
        """
        Generate monitoring report

        Args:
            days_back: Number of days to look back for report. Default is 7.
            report_file_path: Path to save the report file
        """
        dbt_target_path = target_path or PATH_TO_DBT_TARGET

        config = Config(
            config_dir=PATH_TO_DBT_PROJECT,
            profiles_dir=PATH_TO_DBT_PROJECT,
            project_dir=PATH_TO_DBT_PROJECT,
            profile_target=ENV_SHORT_NAME,
            target_path=dbt_target_path,
            gcs_bucket_name=DATA_GCS_BUCKET_NAME,
            env=ENV_SHORT_NAME,
            run_dbt_deps_if_needed=True,
        )
        anonymous_tracking = AnonymousCommandLineTracking(config)
        config.validate_send_report()

        data_monitoring = DataMonitoringReport(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=False,
            disable_samples=False,
        )
        return data_monitoring.send_report(
            days_back=days_back,
            test_runs_amount=720,
            disable_passed_test_metrics=False,
            should_open_browser=False,
            exclude_elementary_models=False,
            project_name=None,
            remote_file_path=report_file_path,
            disable_html_attachment=True,
            include_description=False,
        )

    def send_monitoring_report(
        self,
        slack_channel: str,
        slack_token: str,
        slack_group_alerts_by: str = "table",
        global_suppression_interval: int = 24,
        days_back: int = 1,
        target_path: str = None,
    ) -> bool:
        """
        Send monitoring report to slack channel

        Args:
            slack_channel: Slack channel name
            slack_token: Slack token
            slack_group_alerts_by: Slack group alerts by table or model. Default is table.
            global_suppression_interval: Number of hours to wait before sending the same alert again. Default is 24.
            days_back: Number of days to look back for alerts. Default is 1.
        """

        dbt_target_path = target_path or PATH_TO_DBT_TARGET

        config = Config(
            config_dir=PATH_TO_DBT_PROJECT,
            profiles_dir=PATH_TO_DBT_PROJECT,
            project_dir=PATH_TO_DBT_PROJECT,
            profile_target=ENV_SHORT_NAME,
            target_path=dbt_target_path,
            env=ENV_SHORT_NAME,
            run_dbt_deps_if_needed=True,
            slack_channel_name=slack_channel,
            slack_token=slack_token,
            slack_group_alerts_by=slack_group_alerts_by,
            report_url=ELEMENTARY_REPORT_URL,
        )
        anonymous_tracking = AnonymousCommandLineTracking(config)
        config.validate_monitor()
        data_monitoring = DataMonitoringAlerts(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=False,
            send_test_message_on_success=True,
            disable_samples=False,
            selector_filter=FiltersSchema(),
            global_suppression_interval=global_suppression_interval,
            override_config=False,
        )

        data_monitoring.run_alerts(
            days_back=days_back,
            dbt_full_refresh=False,
            dbt_vars=None,
        )
