import pandas as pd
from datetime import datetime
import time
import subprocess
import typer
from utils import (
    BIGQUERY_CLEAN_DATASET,
    MODELS_RESULTS_TABLE_NAME,
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
)

TRAIN_DIR = "/home/airflow/train"


def save_experiment(experiment_name, model_name, serving_container, run_id):

    log_results = {
        "execution_date": datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "model_name": model_name,
        "model_type": "custom",
        "run_id": run_id,
        "run_start_time": int(time.time() * 1000.0),
        "run_end_time": int(time.time() * 1000.0),
        "artifact_uri": None,
        "serving_container": serving_container,
        "precision_at_10": 0.0,
        "recall_at_10": 0.0,
        "coverage_at_10": 0.0,
    }
    pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
        project_id=f"{GCP_PROJECT_ID}",
        if_exists="append",
    )


def deploy_container(serving_container):
    command = f"sh ./deploy_to_docker_registery.sh {TRAIN_DIR} {ENV_SHORT_NAME} {serving_container}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def main(
    experiment_name: str = typer.Option(
        None,
        help="Name of the experiment",
    ),
    model_name: str = typer.Option(
        None,
        help="Name of the model",
    ),
) -> None:
    yyyymmdd = datetime.now().strftime("%Y%m%d")
    serving_container = f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{ENV_SHORT_NAME}_v{yyyymmdd}"
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=yyyymmdd)


if __name__ == "__main__":
    typer.run(main)
