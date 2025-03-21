import json

import mlflow
import typer
from catboost import CatBoostClassifier

from constants import (
    CONFIGS_PATH,
    ENV_SHORT_NAME,
    MLFLOW_RUN_ID_FILENAME,
    STORAGE_PATH,
)
from utils.data_collect_queries import read_from_gcs
from utils.mlflow_tools import connect_remote_mlflow


def train(
    model_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    ),
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    training_table_name: str = typer.Option(
        "compliance_training_data",
        help="BigQuery table containing compliance training data",
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
):
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    train_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=training_table_name
    )
    train_data_labels = train_data.target.tolist()
    train_data = train_data.drop(columns=["target"])
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

    # Mlflow logging
    connect_remote_mlflow()
    experiment_name = f"{model_name}_v1.0_{ENV_SHORT_NAME}"
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name):
        run_uuid = mlflow.active_run().info.run_uuid
        with open(f"{MLFLOW_RUN_ID_FILENAME}.txt", mode="w") as file:
            file.write(run_uuid)
        mlflow.log_params(
            params={
                "environment": ENV_SHORT_NAME,
                "train_item_count": len(train_data),
                "train_validated_count": train_data_labels.count(1),
                "train_rejected_count": train_data_labels.count(0),
            }
        )

        # Log Catboost model in API
        mlflow.catboost.log_model(
            cb_model=model,
            artifact_path=f"registry_{ENV_SHORT_NAME}",
            registered_model_name=f"{model_name}_{ENV_SHORT_NAME}",
        )


if __name__ == "__main__":
    typer.run(train)
