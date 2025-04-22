from common.alerts.task_fail import task_fail_slack_alert
from common.hooks.gce import on_failure_callback_stop_vm


def on_failure_vm_callback(context):
    """
    Stop the VM and send a slack alert
    """
    on_failure_callback_stop_vm(context)
    task_fail_slack_alert(context)


def on_failure_base_callback(context):
    """
    Send a slack alert based on the ENV_SHORT_NAME
    """
    task_fail_slack_alert(context)
