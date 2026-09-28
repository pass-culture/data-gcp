"""Step 0 of the embedding pipeline: open the MLflow run for this DAG run.

One MLflow run is created per DAG run and shared by every vector's ``embed``
step. This command creates it, records which Airflow run triggered it, and
writes the resulting ``run_id`` to a local file. Each later ``embed`` process
reads that file and resumes the same run to append its own vector's config +
counts (they run sequentially on the same VM).

Run from the job root:
    uv run python -m cli.mlflow_run \
        --airflow-run-id manual__2026-09-24T... \
        --embed-all
"""

import mlflow
import typer
from loguru import logger
from src.constants import ENV_SHORT_NAME, MLFLOW_EXPERIMENT_NAME
from src.mlflow_utils import connect_to_mlflow, get_mlflow_experiment, write_run_id

app = typer.Typer(help="Manage the item embedding MLflow run.")


@app.command()
def start(
    embed_all: bool = typer.Option(  # noqa: FBT001
        False, help="Whether this run re-embeds the full catalogue."
    ),
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
            }
        )
        mlflow.log_param("embed_all", embed_all)

    write_run_id(run_id)
    logger.info(f"Started MLflow run '{run_id}' in experiment '{experiment.name}'.")


if __name__ == "__main__":
    app()
