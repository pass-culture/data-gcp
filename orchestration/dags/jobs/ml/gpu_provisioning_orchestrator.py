import re
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common.config import ENV_SHORT_NAME, GCE_ZONES
from common.operators.gce import StartGCEOperator

from jobs.crons import SCHEDULE_DICT


# ============================================================================
# ZONE SWEEPING
# ============================================================================
class StartGCEZoneSweeperOperator(StartGCEOperator):
    """
    Cycles through a list of zones immediately within a single task run.
    If all zones fail, it raises an AirflowException to trigger the hourly retry.
    """

    def __init__(self, gce_zones: list[str], *args: Any, **kwargs: Any) -> None:
        # Initialize the base operator with the first zone as a placeholder
        super().__init__(gce_zone=gce_zones[0], **kwargs)
        self.gce_zones = gce_zones

    def execute(self, context: Any) -> None:
        last_exception = None
        try_number: int = context["task_instance"].try_number

        # RESTORED: Initial attempt logging
        self.log.info(f"Starting execution attempt #{try_number}")

        for zone in self.gce_zones:
            self.gce_zone = zone
            self.log.info(f"Targeting zone: '{self.gce_zone}'...")

            try:
                super().execute(context)

                # Success confirmation log
                self.log.info(f"Success! VM allocated in zone: '{self.gce_zone}'")

                # Populate the default XCom slot.
                ti = context["task_instance"]
                ti.xcom_push(key="allocated_zone", value=self.gce_zone)

                return  # Gracefully exits the method returning None

            except Exception as e:
                # Detailed loop warning log for stockouts
                self.log.warning(
                    f"Zone '{self.gce_zone}' failed due to stockout or error: {e}. "
                    f"Pivoting to next zone immediately..."
                )
                last_exception = e
                continue

        # sweep not successful exception message
        raise AirflowException(
            f"All configured zones ({self.gce_zones}) were exhausted and failed to allocate "
            f"GPU resources. Last error: {last_exception}"
        )


# ============================================================================
# CENTRALIZED GPU CONFIGURATION (Keyed by target_dag_id)
# ============================================================================

BRANCH_DEFAULT = "production" if ENV_SHORT_NAME == "prod" else "master"

GPU_WORKLOADS: dict[str, dict[str, Any]] = {
    "item_embedding": {
        "schedule": SCHEDULE_DICT.get("item_embedding"),
        "instance_type": {
            "dev": "n1-standard-4",
            "stg": "n1-standard-16",
            "prod": "n1-standard-16",
        }[ENV_SHORT_NAME],
        "gpu_type": "nvidia-tesla-t4",
        "gpu_count": 1 if ENV_SHORT_NAME == "dev" else 4,
        "gce_zones": GCE_ZONES,
        "downstream_params": {
            "branch": BRANCH_DEFAULT,
            "embed_all": True,
            "config_file_name": "default",
        },
    },
    # Add other GPU dags here as needed...
}

MAX_RETRY_DELAY_HOURS = 12
RETRY_DELAY_MINUTES = 30
MAX_RETRY = int(MAX_RETRY_DELAY_HOURS * 60 / RETRY_DELAY_MINUTES)

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1),
    "depends_on_past": False,
}

# ============================================================================
# DYNAMIC DAG GENERATION (The Factory Loop)
# ============================================================================
for target_dag_id, gce_config in GPU_WORKLOADS.items():
    # 1. fetch cron schedule
    workload_schedules = gce_config.get("schedule", {})
    if isinstance(workload_schedules, dict):
        cron_schedule = workload_schedules.get(ENV_SHORT_NAME)
    elif workload_schedules:
        cron_schedule = workload_schedules
    else:
        cron_schedule = None

    # 2. unique identifier for this orchestrator DAG
    orchestrator_dag_id = f"orchestrator_{target_dag_id}"

    # Sanitize GCE naming
    sanitized_dag_id = re.sub(r"[^a-zA-Z0-9]", "-", target_dag_id).lower()
    instance_name_template = f"gpu-vm-{sanitized_dag_id}-{{{{ ts_nodash | lower }}}}"

    with DAG(
        dag_id=orchestrator_dag_id,
        default_args=DEFAULT_ARGS,
        schedule=cron_schedule,
        catchup=False,
        max_active_runs=1,
        tags=["gpu_dispatcher"],
    ) as generated_dag:
        # Attempt to provision the VM (round-robin region attempt, then sleep before retry + extensive timeout)
        launch_gpu_vm = StartGCEZoneSweeperOperator(
            task_id="launch_gpu_vm",
            preemptible=False,
            instance_name=instance_name_template,
            instance_type=gce_config["instance_type"],
            gpu_type=gce_config["gpu_type"],
            gpu_count=gce_config["gpu_count"],
            gce_zones=gce_config["gce_zones"],
            retries=MAX_RETRY,
            retry_delay=timedelta(minutes=RETRY_DELAY_MINUTES),
        )

        # Build downstream configuration payload dynamically
        trigger_conf = {
            "instance_name": instance_name_template,
            "instance_type": gce_config["instance_type"],
            "gpu_type": gce_config["gpu_type"],
            "gpu_count": int(gce_config["gpu_count"]),
            "gce_zone": "{{ task_instance.xcom_pull(task_ids='launch_gpu_vm', key='allocated_zone') }}",
        }
        trigger_conf.update(gce_config.get("downstream_params", {}))

        # Task B: Trigger the actual execution workflow
        trigger_processing = TriggerDagRunOperator(
            task_id="trigger_downstream_dag",
            trigger_dag_id=target_dag_id,
            conf=trigger_conf,
            wait_for_completion=False,  # task succeed if external dag is triggered even if it failed
        )

        launch_gpu_vm >> trigger_processing

    # Register the dynamically created DAG into the global namespace
    globals()[orchestrator_dag_id] = generated_dag
