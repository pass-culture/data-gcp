"""Open the pipeline-wide MLflow run shared by every domain command.

One MLflow run is created per pipeline run (a full refit or an incremental
run) and shared by every domain's commands. This creates it, records which
Airflow run triggered it and which config file this run uses, and writes the
resulting run_id to a local file; each later domain command reads that file
(``src.common.run.read_run_id``) to resume the same run.

Run from the job root:
    uv run python main.py common start --split-level 1 --config-file-name default
"""

import mlflow
import typer
from loguru import logger

from src.common.constants import ENV_SHORT_NAME, MLFLOW_EXPERIMENT_NAME
from src.common.mlflow import connect_to_mlflow, get_mlflow_experiment
from src.common.run import write_run_id

app = typer.Typer(help="Manage the topics pipeline's shared MLflow run.")


@app.command()
def start(
    config_file_name: str = typer.Option(
        "default", help="Which configs/*.yaml this run uses."
    ),
    split_level: int = typer.Option(1, help="Corpus split level for this run."),
    airflow_run_id: str = typer.Option(
        "", help="Airflow run_id that triggered this DAG run (for traceability)."
    ),
) -> None:
    connect_to_mlflow()
    experiment = get_mlflow_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run(
        experiment_id=experiment.experiment_id,
        run_name=airflow_run_id or None,
    ) as run:
        run_id = run.info.run_id
        mlflow.set_tags(
            {
                "env": ENV_SHORT_NAME,
                "airflow_run_id": airflow_run_id,
                "split_level": split_level,
            }
        )
        mlflow.log_param("config_file_name", config_file_name)

    write_run_id(run_id)
    logger.info(f"Started MLflow run '{run_id}' in experiment '{experiment.name}'.")


if __name__ == "__main__":
    app()
