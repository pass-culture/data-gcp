import mlflow
import pandas as pd
import typer
from catboost import CatBoostClassifier
from loguru import logger
from sklearn.model_selection import train_test_split

from commons.constants import ENV_SHORT_NAME
from commons.mlflow_tools import connect_remote_mlflow
from offer_categorization.config import features


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment


def split_train_val(
    train_data_clean: pd.DataFrame, train_data_labels: pd.Series
) -> pd.DataFrame:
    label_counts = train_data_labels.value_counts()
    rare_classes = label_counts[label_counts < 2].index
    train_data_clean = train_data_clean.loc[~train_data_clean.isin(rare_classes)]
    train_data_labels = train_data_labels[~train_data_labels.isin(rare_classes)]

    return train_test_split(
        train_data_clean,
        train_data_labels,
        test_size=0.1,
        random_state=42,
        stratify=train_data_labels,
    )


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
    num_boost_round: int = typer.Option(..., help="Number of iterations"),
) -> None:
    logger.info("Training model...")
    features_config = features["default"]
    data = pd.read_parquet(training_table_path)
    data_labels = data.offer_subcategory_id
    data_clean = data.drop(columns=["label", "offer_subcategory_id"])

    # Split the data into training and validation sets
    (
        train_data,
        val_data,
        labels_train,
        labels_val,
    ) = split_train_val(data_clean, data_labels)

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
