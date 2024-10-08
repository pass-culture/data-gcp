import mlflow
import pandas as pd
import typer
from catboost import CatBoostClassifier
from loguru import logger

from commons.constants import ENV_SHORT_NAME
from commons.mlflow_tools import connect_remote_mlflow
from offer_categorization.config import features


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment


def main(
    model_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    ),
    training_table_path: str = typer.Option(
        ...,
        help="BigQuery table containing compliance training data",
    ),
    validation_table_path: str = typer.Option(
        ...,
        help="BigQuery table containing compliance validation data",
    ),
    run_name: str = typer.Option(..., help="Name of the MLflow run if set"),
    num_boost_round: int = typer.Option(..., help="Number of iterations"),
) -> None:
    logger.info("Training model...")
    features_config = features["default"]
    train, val = (
        pd.read_parquet(training_table_path),
        pd.read_parquet(validation_table_path),
    )
    labels_train, labels_val = train.offer_subcategory_id, val.offer_subcategory_id
    train_data, val_data = (
        train.drop(columns=["label", "offer_subcategory_id"]),
        val.drop(columns=["label", "offer_subcategory_id"]),
    )

    logger.info("Init classifier..")
    # Add auto_class_weights to balance
    model = CatBoostClassifier(
        one_hot_max_size=65,
        loss_function="MultiClass",
        auto_class_weights="Balanced",
        num_boost_round=num_boost_round,
    )
    logger.info("Fitting model..")
    ## Model Fit
    model.fit(
        train_data,
        labels_train,
        cat_features=features_config["catboost_features_types"]["cat_features"],
        text_features=features_config["catboost_features_types"]["text_features"],
        embedding_features=features_config["catboost_features_types"][
            "embedding_features"
        ],
        verbose=True,
        early_stopping_rounds=50,
        eval_set=(
            val_data,
            labels_val,
        ),
    )

    logger.info("Log model to MLFlow..")
    connect_remote_mlflow()
    experiment_name = f"{model_name}_v1.0_{ENV_SHORT_NAME}"
    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        mlflow.log_params(
            params={
                "environment": ENV_SHORT_NAME,
                "train_item_count": train_data.shape[0],
                "subcategory population": train_data["offer_subcategory_id"]
                .value_counts()
                .to_dict(),
            }
        )
        mlflow.catboost.log_model(
            cb_model=model,
            artifact_path=f"registry_{ENV_SHORT_NAME}",
            registered_model_name=f"{model_name}_{ENV_SHORT_NAME}",
        )


if __name__ == "__main__":
    typer.run(main)
