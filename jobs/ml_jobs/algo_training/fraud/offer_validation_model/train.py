import json

import mlflow
import pandas as pd
from catboost import CatBoostClassifier
from utils.constants import CONFIGS_PATH, MLFLOW_CLIENT_ID, MODEL_DIR, STORAGE_PATH
from utils.tools import connect_remote_mlflow, get_mlflow_experiment


def train(
    experiment_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    ),
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    training_table_name: str = typer.Option(
        "recommendation_training_data", help="BigQuery table containing training data"
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
):
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    train_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=training_table_name
    )
    train_data_labels = train_data.target.tolist()
    train_data = train_data.drop(columns=["target"])
    # train_pool = Pool(train_data,
    #                 train_data_labels,
    #                 cat_features=features["catboost_features_types"]["cat_features"],
    #                 text_features=features["catboost_features_types"]["text_features"],
    #                 embedding_features=features["catboost_features_types"]["embedding_features"])
    model = CatBoostClassifier(one_hot_max_size=65)
    ## Model Fit
    model.fit(
        train_data,
        train_data_labels,
        cat_features=features["catboost_features_types"]["cat_features"],
        text_features=features["catboost_features_types"]["text_features"],
        embedding_features=features["catboost_features_types"]["embedding_features"],
        verbose=True,
    )

    connect_remote_mlflow(MLFLOW_CLIENT_ID, env="ehp")
    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment, run_name=run_name):
        mlflow.log_params(
            params={
                "environment": "EHP",
                "train_item_count": len(train_data),
                "train_validated_count": train_data_labels.count(1),
                "train_rejected_count": train_data_labels.count(0),
            }
        )
        mlflow.catboost.log_model(
            cb_model=model,
            artifact_path="registry_dev",
            registered_model_name="validation_model_dev",
        )
