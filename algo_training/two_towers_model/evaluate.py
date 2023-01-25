from datetime import datetime

import mlflow.tensorflow
import pandas as pd
import tensorflow as tf
import typer
from loguru import logger

from two_towers_model.models.match_model import MatchModel
from utils.constants import (
    STORAGE_PATH,
    ENV_SHORT_NAME,
    BIGQUERY_CLEAN_DATASET,
    MODELS_RESULTS_TABLE_NAME,
    GCP_PROJECT_ID,
    SERVING_CONTAINER,
    MODEL_NAME,
    EXPERIMENT_NAME,
)
from utils.evaluate import evaluate
from utils.mlflow_tools import connect_remote_mlflow
from utils.secrets_utils import get_secret


def main(
    experiment_name: str = typer.Option(
        EXPERIMENT_NAME, help="Name of the experiment on MLflow"
    ),
    model_name: str = typer.Option(MODEL_NAME, help="Name of the model to evaluate"),
    training_dataset_name: str = typer.Option(
        "positive_data_train", help="Name of the training dataset in storage"
    ),
    test_dataset_name: str = typer.Option(
        "positive_data_test", help="Name of the test dataset in storage"
    ),
):
    logger.info("-------EVALUATE START------- ")
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id

    with mlflow.start_run(run_id=run_id) as run:
        artifact_uri = mlflow.get_artifact_uri("model")
        loaded_model = tf.keras.models.load_model(
            artifact_uri,
            custom_objects={"MatchModel": MatchModel},
            compile=False,
        )
        log_results = {
            "execution_date": datetime.now().isoformat(),
            "experiment_name": experiment_name,
            "model_name": model_name,
            "model_type": "tensorflow",
            "run_id": run_id,
            "run_start_time": run.info.start_time,
            "run_end_time": run.info.start_time,
            "artifact_uri": artifact_uri,
            "serving_container": SERVING_CONTAINER,
        }

        pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
            f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
            project_id=f"{GCP_PROJECT_ID}",
            if_exists="append",
        )

        metrics = evaluate(
            loaded_model,
            STORAGE_PATH,
            training_dataset_name,
            test_dataset_name,
        )
        mlflow.log_metrics(metrics)

        print("------- EVALUATE DONE -------")


if __name__ == "__main__":
    typer.run(main)
