import json
import os

import mlflow
import pandas as pd
import typer
from catboost import Pool
from mlflow import MlflowClient

from constants import (
    CONFIGS_PATH,
    ENV_SHORT_NAME,
    MLFLOW_RUN_ID_FILENAME,
    STORAGE_PATH,
)
from utils.data_collect_queries import read_from_gcs
from utils.mlflow_tools import connect_remote_mlflow


def evaluate(
    model_name: str = typer.Option(
        "compliance_default", help="Model name for the training"
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
        f"{CONFIGS_PATH}/{config_file_name}.json",
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
    connect_remote_mlflow()
    model = mlflow.catboost.load_model(
        model_uri=f"models:/{model_name}_{ENV_SHORT_NAME}/latest"
    )
    metrics = model.eval_metrics(
        eval_pool,
        ["Accuracy", "BalancedAccuracy", "Precision", "Recall", "BalancedErrorRate"],
        ntree_start=0,
        ntree_end=1,
        eval_period=1,
        thread_count=-1,
    )
    # Format metrics for MLFlow
    for key in metrics:
        metrics[key] = metrics[key][0]

    # Build and save probability distribution
    figure_folder = "probability_distribution/"
    save_probability_distribution_plot(
        model, eval_pool, eval_data_labels, figure_folder
    )
    experiment_name = f"{model_name}_v1.0_{ENV_SHORT_NAME}"
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    with open(f"{MLFLOW_RUN_ID_FILENAME}.txt") as file:
        run_id = file.read()
    with mlflow.start_run(experiment_id=experiment_id, run_id=run_id):
        mlflow.log_metrics(metrics)
        mlflow.log_artifacts(figure_folder, "probability_distribution")
    client = MlflowClient()
    latest_version = client.get_latest_versions(
        f"{model_name}_{ENV_SHORT_NAME}", stages=["None"]
    )[0].version
    client.transition_model_version_stage(
        name=f"{model_name}_{ENV_SHORT_NAME}",
        version=latest_version,
        stage="Production",
    )


def save_probability_distribution_plot(model, eval_pool, data_labels, figure_folder):
    os.makedirs(figure_folder, exist_ok=True)
    probability_distribution = model.predict(
        eval_pool,
        prediction_type="Probability",
        ntree_start=0,
        ntree_end=0,
        thread_count=-1,
        verbose=None,
    )

    df_proba = pd.DataFrame(
        {
            "target": data_labels,
            "probability_validated": [
                prob[1] for prob in list(probability_distribution)
            ],
            "probability_rejected": [
                prob[0] for prob in list(probability_distribution)
            ],
        }
    )

    ax = df_proba.query("target==1").probability_validated.hist(
        histtype="barstacked", stacked=True
    )
    ax = df_proba.query("target==0").probability_validated.hist(
        histtype="barstacked", stacked=True
    )
    fig = ax.get_figure()
    fig.savefig(f"{figure_folder}/probability_distribution.pdf")


if __name__ == "__main__":
    typer.run(evaluate)
