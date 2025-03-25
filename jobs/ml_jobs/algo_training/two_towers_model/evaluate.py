from datetime import datetime

import mlflow.tensorflow
import pandas as pd
import typer
from loguru import logger

from commons.constants import (
    BIGQUERY_CLEAN_DATASET,
    GCP_PROJECT_ID,
    MLFLOW_RUN_ID_FILENAME,
    MODEL_DIR,
    MODEL_NAME,
    MODELS_RESULTS_TABLE_NAME,
    STORAGE_PATH,
)
from commons.mlflow_tools import connect_remote_mlflow
from two_towers_model.utils.evaluate import (
    evaluate,
)


def main(
    experiment_name: str = typer.Option(None, help="Name of the experiment on MLflow"),
    model_name: str = typer.Option(MODEL_NAME, help="Name of the model to evaluate"),
    train_dataset_name: str = typer.Option(
        "recommendation_training_data", help="Name of the training dataset in storage"
    ),
    test_dataset_name: str = typer.Option(
        "recommendation_test_data", help="Name of the test dataset in storage"
    ),
    list_k: list[int] = typer.Option(
        [10, 50, 100, 250, 1000],
        help="List of k values (top-k cutoff) for metrics evaluation.",
    ),
    all_users: bool = typer.Option(
        False, help="Whether to evaluate for all users or not"
    ),
    dummy: bool = typer.Option(
        False, help="Whether to evaluate metrics on dummy models or not"
    ),
    quantile_threshold: float = typer.Option(
        0.99,
        help="Threshold to consider top X% most popular items (0-1 range) in recommend popular dummy model",
    ),
):
    logger.info("-------EVALUATE START------- ")
    connect_remote_mlflow()
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    with open(f"{MODEL_DIR}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="r") as file:
        run_id = file.read()

    with mlflow.start_run(experiment_id=experiment_id, run_id=run_id) as run:
        log_results = {
            "execution_date": datetime.now().isoformat(),
            "experiment_name": experiment_name,
            "model_name": model_name,
            "model_type": "tensorflow",
            "run_id": run_id,
            "run_start_time": run.info.start_time,
            "run_end_time": run.info.start_time,
        }

    pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
        project_id=f"{GCP_PROJECT_ID}",
        if_exists="append",
    )

    metrics = evaluate(
        storage_path=STORAGE_PATH,
        train_dataset_name=train_dataset_name,
        test_dataset_name=test_dataset_name,
        list_k=list_k,
        all_users=all_users,
        dummy=dummy,
        quantile_threshold=quantile_threshold,
    )

    connect_remote_mlflow()
    with mlflow.start_run(
        experiment_id=experiment_id, run_id=run_id, nested=True
    ) as run:
        mlflow.log_metrics(metrics)

    print("------- EVALUATE DONE -------")


if __name__ == "__main__":
    typer.run(main)
