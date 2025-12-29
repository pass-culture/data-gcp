from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.hooks.monitoring import ElementaryReport


class GenerateElementaryReportOperator(BaseOperator):
    template_fields = [
        "report_file_path",
        "days_back",
    ]

    @apply_defaults
    def __init__(
        self,
        report_file_path: str,
        days_back: int,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.report_file_path = report_file_path
        self.days_back = days_back

    def execute(self, context):
        hook = ElementaryReport()
        results = hook.generate_report(self.report_file_path, self.days_back)
        if results:
            self.log.info("Elementary report generated successfully")
        else:
            self.log.error("Elementary report generation failed")
            raise AirflowFailException("Elementary report generation failed")


class SendElementaryMonitoringReportOperator(BaseOperator):
    template_fields = [
        "days_back",
        "slack_group_alerts_by",
        "global_suppression_interval",
        "send_slack_report",
    ]

    @apply_defaults
    def __init__(
        self,
        slack_channel: str,
        slack_token: str,
        days_back: int = 1,
        slack_group_alerts_by: str = "table",
        global_suppression_interval: int = 24,
        send_slack_report: str = "True",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.slack_channel = slack_channel
        self.slack_token = slack_token
        self.days_back = days_back
        self.slack_group_alerts_by = slack_group_alerts_by
        self.global_suppression_interval = global_suppression_interval
        self.send_slack_report = send_slack_report

    def _parse_send_slack_report(self):
        self.log.info(f"send_slack_report: {self.send_slack_report}")
        if isinstance(self.send_slack_report, bool):
            return self.send_slack_report
        elif isinstance(self.send_slack_report, str):
            if self.send_slack_report == "True":
                return True
            elif self.send_slack_report == "False":
                return False
            else:
                raise ValueError(
                    f"Invalid value for send_slack_report: {self.send_slack_report}"
                )
        else:
            raise ValueError(
                f"Invalid value for send_slack_report: {self.send_slack_report}"
            )

    def execute(self, context):
        send_slack_report = self._parse_send_slack_report()
        self.log.info(f"Sending Slack report: {send_slack_report}")
        if not send_slack_report:
            self.log.info("Skipping Slack report sending")
            return

        hook = ElementaryReport()
        results = hook.send_monitoring_report(
            self.slack_channel,
            self.slack_token,
            days_back=self.days_back,
            slack_group_alerts_by=self.slack_group_alerts_by,
            global_suppression_interval=self.global_suppression_interval,
        )
        if results:
            self.log.info("Elementary monitoring report sent successfully")
        else:
            self.log.error("Elementary monitoring report sending failed")
