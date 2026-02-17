from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
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

DATE = "{{ ts_nodash }}"
DAG_NAME = "finance_experimental_pricing_forecast"

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

script_params = {
    "model_type": "prophet",
    "model_name": "daily_pricing",
    "train_start_date": "2022-01-01",
    "backtest_start_date": "2025-09-01",
    "backtest_end_date": "2025-12-31",
    "forecast_horizon_date": "2026-12-31",
    "run_backtest": True,
    "experiment_name": f"finance_pricing_forecast_v0_{ENV_SHORT_NAME}",
    "dataset": f"ml_finance_{ENV_SHORT_NAME}",
}

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
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "model_type": Param(
            default=script_params["model_type"],
            type="string",
            enum=["prophet"],
            description="Type of model to use (e.g. prophet)",
        ),
        "model_name": Param(
            default=script_params["model_name"],
            type="string",
            enum=["daily_pricing", "weekly_pricing"],
            description="Name of the model configuration",
        ),
        "train_start_date": Param(
            default=script_params["train_start_date"],
            type="string",
            description="In-sample start date (YYYY-MM-DD).",
        ),
        "backtest_start_date": Param(
            default=script_params["backtest_start_date"],
            type="string",
            description="Out-of-sample start date (YYYY-MM-DD).",
        ),
        "backtest_end_date": Param(
            default=script_params["backtest_end_date"],
            type="string",
            description="Out-of-sample end date (YYYY-MM-DD)",
        ),
        "forecast_horizon_date": Param(
            default=script_params["forecast_horizon_date"],
            type="string",
            description="Forecast horizon end date (YYYY-MM-DD)",
        ),
        "run_backtest": Param(
            default=script_params["run_backtest"],
            type="boolean",
            description="Whether to evaluate on backtest data",
        ),
        "dataset": Param(
            default=script_params["dataset"],
            type="string",
            description="BigQuery dataset containing the training data",
        ),
        "experiment_name": Param(
            default=script_params["experiment_name"],
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
        dag=dag,
    )

    fit_model = SSHGCEOperator(
        task_id="fit_model",
        instance_name="{{ params.instance_name }}",
        base_dir=dag_config["BASE_DIR"],
        command="""
            uv run python main.py \
                --model-type "{{ params.model_type }}" \
                --model-name "{{ params.model_name }}" \
                --train-start-date "{{ params.train_start_date }}" \
                --backtest-start-date "{{ params.backtest_start_date }}" \
                --backtest-end-date "{{ params.backtest_end_date }}" \
                --forecast-horizon-date "{{ params.forecast_horizon_date }}" \
                --experiment-name "{{ params.experiment_name }}" \
                --dataset "{{ params.dataset }}"
        """,
    )

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule="none_failed",
    )

    send_slack_notif_success = SendSlackMessageOperator(
        task_id="send_slack_notif_success",
        webhook_token=SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN,
        trigger_rule="none_failed",
        block=create_finance_pricing_forecast_slack_block(
            experiment_name="{{ params.experiment_name }}",
            mlflow_url=MLFLOW_URL,
            env_short_name=ENV_SHORT_NAME,
        ),
    )

    (
        start
        >> gce_instance_start
        >> install_dependencies
        >> fit_model
        >> gce_instance_delete
        >> send_slack_notif_success
    )
