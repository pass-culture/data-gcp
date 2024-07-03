import pandas as pd
from catboost import Pool
import mlflow
from mlflow import MlflowClient
import typer
from sklearn.metrics import classification_report, top_k_accuracy_score
from utils.mlflow_tools import connect_remote_mlflow
from offer_categorization.config import features
from utils.secrets_utils import get_secret
from utils.mlflow_tools import connect_remote_mlflow
from loguru import logger
from utils.constants import (
    ENV_SHORT_NAME,
)

app = typer.Typer()


def get_mlflow_experiment(experiment_name: str):
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(name=experiment_name)
        experiment = mlflow.get_experiment_by_name(experiment_name)
    return experiment


def get_label_mapping(df: pd.DataFrame) -> pd.DataFrame:
    ll = sorted(df.offer_subcategory_id.unique())
    return pd.DataFrame({"offer_subcategory_id": ll, "label": list(range(len(ll)))})


@app.command()
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
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    logger.info("Load model..")
    model = mlflow.catboost.load_model(
        model_uri=f"models:/{model_name}_{ENV_SHORT_NAME}/latest"
    )
    # label_mapping = get_label_mapping(test_data)
    # target_names = [
    #     label_mapping.iloc[idx]["offer_subcategory_id"] for idx in model.classes_
    # ]

    logger.info("Evaluate model..")
    y_true = test_data_labels
    y_pred = model.predict(eval_pool)
    logger.info("Generate classification report..")
    # classification_report = classification_report(
    #     y_true, y_pred, target_names=target_names
    # )
    classification_report_output = classification_report(y_true, y_pred)
    y_pred_proba = model.predict_proba(eval_pool)
    logger.info("Generate top_k_accuracy_score..")
    top_k_accuracy_score_output = top_k_accuracy_score(
        y_true, y_pred_proba, k=3, labels=model.classes_
    )

    logger.info("Generate feature importance..")
    model_feature_importance = model.get_feature_importance(prettified=True)

    logger.info("Eval set of metrics..")
    metrics = model.eval_metrics(
        eval_pool,
        ["MultiClass", "MultiClassOneVsAll", "Accuracy", "Precision"],
        ntree_start=0,
        ntree_end=1,
        eval_period=1,
        thread_count=-1,
    )
    # Format metrics for MLFlow
    metrics_ml = {}
    for key in metrics.keys():
        key_mlf = key.replace(":", "_")
        key_mlf = key_mlf.replace("=", "_")
        metrics_ml[key_mlf] = metrics[key][0]
    logger.info("Log metrics..")
    experiment_name = f"{model_name}_v1.0_{ENV_SHORT_NAME}"
    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        mlflow.log_metrics(metrics_ml)
        # mlflow.log_artifacts(classification_report_output, "classification_report ")
        # mlflow.log_artifacts(top_k_accuracy_score_output, "top_k_accuracy_score")
        # mlflow.log_artifacts(model_feature_importance, "feature importance")
        # mlflow.log_artifacts(figure_folder, "probability_distribution")
        client = mlflow.MlflowClient()

        latest_version = client.get_latest_versions(
            f"{model_name}_{ENV_SHORT_NAME}", stages=["None"]
        )[0].version
        client.transition_model_version_stage(
            name=f"{model_name}_{ENV_SHORT_NAME}",
            version=latest_version,
            stage="Production",
        )


if __name__ == "__main__":
    app()
