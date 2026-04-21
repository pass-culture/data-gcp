from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.alerts import SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN
from common.alerts.ml_training import create_finance_pricing_forecast_slack_block
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    ML_BUCKET_TEMP,
    MLFLOW_URL,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.operators.slack import SendSlackMessageOperator

from jobs.crons import SCHEDULE_DICT

DATE = "{{ ts_nodash }}"
DAG_NAME = "finance_pricing_forecast"

# Environment variables to export before running commands
dag_config = {
    "STORAGE_PATH": f"gs://{ML_BUCKET_TEMP}/ml_finance_pricing_forecast/{ENV_SHORT_NAME}/{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/finance",
}

# Params
gce_params = {
    "instance_name": f"ml-finance-pricing-forecast-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-4",
        "prod": "n1-standard-4",
    },
}

MODEL_CONFIGS = {
    "prophet_daily_pricing": {
        "model_type": "prophet",
        "model_name": "daily_pricing",
        "dataset": f"ml_finance_{ENV_SHORT_NAME}",
    },
    "prophet_weekly_pricing": {
        "model_type": "prophet",
        "model_name": "weekly_pricing",
        "dataset": f"ml_finance_{ENV_SHORT_NAME}",
    },
}

if ENV_SHORT_NAME == "dev":
    # For dev, force stg dataset
    for config in MODEL_CONFIGS.values():
        config["dataset"] = "ml_finance_stg"

default_args = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
# Run weekly just to test out the DAG then will be monthly

with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Finance Pricing Forecast ML Job",
    schedule_interval=SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME],
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        # Infrastructure params (can be overridden at runtime)
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "experiment_name": Param(
            default=f"finance_pricing_forecast_v0_{ENV_SHORT_NAME}",
            type="string",
            description="MLflow experiment name",
        ),
        "instance_type": Param(
            default=gce_params["instance_type"][ENV_SHORT_NAME],
            type="string",
            description="GCE instance type",
        ),
        "instance_name": Param(
            default=gce_params["instance_name"],
            type="string",
            description="GCE instance name",
        ),
        # Model training params
        "train_start_date": Param(
            default="2022-01-01",
            type="string",
            description="Training start date (YYYY-MM-DD). Must be before first changepoint.",
        ),
        "backtest_days": Param(
            default=120,
            type="integer",
            description="Number of days for backtest period (~4 months)",
        ),
        "forecast_days": Param(
            default=365,
            type="integer",
            description="Number of days for forecast horizon (1 year)",
        ),
        "n_past_runs_to_compare": Param(
            default=6,
            type="integer",
            description="Number of past runs to compare in MLflow for best model selection",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "ml", "dag_name": DAG_NAME},
    )

    install_dependencies = InstallDependenciesOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        branch="{{ params.branch }}",
        retries=2,
        python_version="3.11",
        dag=dag,
    )

    # Create TaskGroup for all model fitting tasks
    with TaskGroup(
        group_id="fit_models", tooltip="Train all pricing forecast models"
    ) as fit_models_group:
        fit_tasks = []
        for model_config_name, config in MODEL_CONFIGS.items():
            fit_model = SSHGCEOperator(
                task_id=f"fit_{model_config_name}",
                instance_name="{{ params.instance_name }}",
                base_dir=dag_config["BASE_DIR"],
                command=f"""
                    uv run python main.py \\
                        --model-type "{config['model_type']}" \\
                        --model-name "{config['model_name']}" \\
                        --train-start-date "{{{{ params.train_start_date }}}}" \\
                        --execution-date "{{{{ ds }}}}" \\
                        --backtest-days {{{{ params.backtest_days }}}} \\
                        --forecast-days {{{{ params.forecast_days }}}} \\
                        --experiment-name "{{{{ params.experiment_name }}}}" \\
                        --dataset "{config['dataset']}" \\
                        --n-past-runs-to-compare "{{{{ params.n_past_runs_to_compare }}}}"
                """,
            )
            fit_tasks.append(fit_model)

        # Chain tasks inside the group sequentially
        chain(*fit_tasks)

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="none_failed",
    )

    # Note: Slack notification uses first model's experiment name
    # Consider updating to list all experiments if needed
    send_slack_notif_success = SendSlackMessageOperator(
        task_id="send_slack_notif_success",
        webhook_token=SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN,
        trigger_rule="none_failed",
        block=create_finance_pricing_forecast_slack_block(
            models=", ".join(MODEL_CONFIGS.keys()),
            mlflow_url=MLFLOW_URL,
            env_short_name=ENV_SHORT_NAME,
        ),
    )

    chain(
        start,
        gce_instance_start,
        install_dependencies,
        fit_models_group,
        gce_instance_delete,
        send_slack_notif_success,
    )
