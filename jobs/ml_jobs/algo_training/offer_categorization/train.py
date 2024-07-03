import pandas as pd
import mlflow
from catboost import CatBoostClassifier
import typer
from loguru import logger
from offer_categorization.config import features
from utils.mlflow_tools import connect_remote_mlflow
from utils.constants import ENV_SHORT_NAME


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment


def main(
    model_name: str = typer.Option(
        "offer_categorization",
        help="MLFlow experiment name",
    ),
    training_table_path: str = typer.Option(
        "compliance_training_data",
        help="BigQuery table containing compliance training data",
    ),
    run_name: str = typer.Option("", help="Name of the MLflow run if set"),
) -> None:
    logger.info("Training model...")
    features_config = features["default"]
    train_data = pd.read_parquet(training_table_path)
    train_data_labels = train_data.offer_subcategory_id.tolist()
    train_data_clean = train_data.drop(columns=["label", "offer_subcategory_id"])
    logger.info("Init classifier..")
    # Add auto_class_weights to balance
    model = CatBoostClassifier(
        one_hot_max_size=65, loss_function="MultiClass", auto_class_weights="Balanced"
    )
    logger.info("Fitting model..")
    ## Model Fit
    model.fit(
        train_data_clean,
        train_data_labels,
        cat_features=features_config["catboost_features_types"]["cat_features"],
        text_features=features_config["catboost_features_types"]["text_features"],
        embedding_features=features_config["catboost_features_types"][
            "embedding_features"
        ],
        verbose=True,
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
