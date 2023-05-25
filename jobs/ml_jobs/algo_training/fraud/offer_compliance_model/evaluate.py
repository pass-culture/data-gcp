import pandas as pd
import mlflow
import json
import typer
from utils.constants import (
    MODEL_DIR,
    STORAGE_PATH,
    EXPERIMENT_NAME,
    ENV_SHORT_NAME,
    MLFLOW_RUN_ID_FILENAME,
)
from fraud.offer_compliance_model.utils.constants import CONFIGS_PATH
from utils.mlflow_tools import connect_remote_mlflow
from utils.secrets_utils import get_secret
from utils.data_collect_queries import read_from_gcs
from catboost import Pool
from mlflow import MlflowClient


def evaluate(
    experiment_name: str = typer.Option(
        EXPERIMENT_NAME, help="Name of the experiment on MLflow"
    ),
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    validation_table_name: str = typer.Option(
        ...,
        help="BigQuery table containing compliance validation data",
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
):
    """
    Preprocessing steps:
        - Fill integer null values with 0
        - Fill string null values with "none"
        - Convert numerical columns to int
    """
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    eval_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=validation_table_name
    )

    eval_data_labels = eval_data.target.tolist()
    eval_data = eval_data.drop(columns=["target"])
    eval_pool = Pool(
        eval_data,
        eval_data_labels,
        cat_features=features["catboost_features_types"]["cat_features"],
        text_features=features["catboost_features_types"]["text_features"],
        embedding_features=features["catboost_features_types"]["embedding_features"],
    )
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    model_name = f"compliance_model_{ENV_SHORT_NAME}"
    model = mlflow.catboost.load_model(model_uri=f"models:/{model_name}/latest")
    metrics = model.eval_metrics(
        eval_pool,
        ["Accuracy", "BalancedAccuracy", "Precision", "Recall", "BalancedErrorRate"],
        ntree_start=0,
        ntree_end=1,
        eval_period=1,
        thread_count=-1,
    )
    # Format metrics for MLFlow
    for key in metrics.keys():
        metrics[key] = metrics[key][0]

    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    with open(f"{MODEL_DIR}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="r") as file:
        run_id = file.read()
    with mlflow.start_run(experiment_id=experiment_id, run_id=run_id) as run:
        mlflow.log_metrics(metrics)

    client = MlflowClient()
    latest_version = client.get_latest_versions(model_name, stages=["None"])[0].version
    client.transition_model_version_stage(
        name=model_name, version=latest_version, stage="Production"
    )

if __name__ == "__main__":
    typer.run(evaluate)
