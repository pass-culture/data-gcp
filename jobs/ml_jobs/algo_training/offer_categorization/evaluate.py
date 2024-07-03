import pandas as pd
from catboost import Pool
import mlflow
import typer
from sklearn.metrics import classification_report, top_k_accuracy_score
from loguru import logger
from utils.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment
from offer_categorization.config import features
from utils.constants import ENV_SHORT_NAME


def main(
    model_name: str = typer.Option(
        "compliance_default", help="Model name for the training"
    ),
    config_name: str = typer.Option("default", help="Model name for the training"),
    validation_table_path: str = typer.Option(
        ...,
        help="BigQuery table containing compliance validation data",
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
) -> None:
    features_description = features[config_name]
    test_data = pd.read_parquet(validation_table_path)

    test_data_labels = test_data.offer_subcategory_id.tolist()
    test_data = test_data.drop(columns=["label", "offer_subcategory_id"])
    eval_pool = Pool(
        test_data,
        test_data_labels,
        cat_features=features_description["catboost_features_types"]["cat_features"],
        text_features=features_description["catboost_features_types"]["text_features"],
        embedding_features=features_description["catboost_features_types"][
            "embedding_features"
        ],
    )
    logger.info("Model evaluation..")
    connect_remote_mlflow()
    logger.info("Load model..")
    model = mlflow.catboost.load_model(
        model_uri=f"models:/{model_name}_{ENV_SHORT_NAME}/latest"
    )

    logger.info("Evaluate model..")
    y_true = test_data_labels
    y_pred = model.predict(eval_pool)

    logger.info("Generate classification report..")
    classification_report_output = classification_report(
        y_true, y_pred, output_dict=True
    )

    logger.info("Generate top_k_accuracy_score..")
    y_pred_proba = model.predict_proba(eval_pool)
    top_k_accuracy_score_output = top_k_accuracy_score(
        y_true, y_pred_proba, k=3, labels=model.classes_
    )

    logger.info("Generate feature importance..")
    model_feature_importance = model.get_feature_importance(prettified=False)
    feature_names = model.feature_names_
    feature_importance_dict = {
        name: importance
        for name, importance in zip(feature_names, model_feature_importance)
    }

    logger.info("log set of metrics..")
    metrics_ml = {}
    metrics_ml["top_3_accuracy_score"] = top_k_accuracy_score_output
    logger.info("Log metrics..")
    experiment_name = f"{model_name}_v1.0_{ENV_SHORT_NAME}"
    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        mlflow.log_metrics(metrics_ml)
        mlflow.log_dict(classification_report_output, "classification_report.json")
        mlflow.log_dict(feature_importance_dict, "feature_importance.json")

        client = mlflow.MlflowClient()
        training_run_id = client.get_latest_versions(f"{model_name}_{ENV_SHORT_NAME}")[
            0
        ].run_id
        model_version = client.get_latest_versions(f"{model_name}_{ENV_SHORT_NAME}")[
            0
        ].version
        client.set_model_version_tag(
            name=f"{model_name}_{ENV_SHORT_NAME}",
            version=model_version,
            key="training_run_id",
            value=training_run_id,
        )
        client.set_registered_model_alias(
            f"{model_name}_{ENV_SHORT_NAME}", "production", model_version
        )


if __name__ == "__main__":
    typer.run(main)
