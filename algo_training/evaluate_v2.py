from datetime import datetime

import tensorflow as tf
import mlflow.tensorflow
import pandas as pd

import typer

from models.v2.utils import load_metrics, save_pca_representation
from tools.data_collect_queries import get_data, get_column_data
from models.v1.match_model import MatchModel
from utils import (
    get_secret,
    connect_remote_mlflow,
    ENV_SHORT_NAME,
    RECOMMENDATION_NUMBER,
    NUMBER_OF_PRESELECTED_OFFERS,
    EVALUATION_USER_NUMBER,
    TRAIN_DIR,
    BIGQUERY_CLEAN_DATASET,
    MODELS_RESULTS_TABLE_NAME,
    GCP_PROJECT_ID,
    SERVING_CONTAINER,
    remove_dir,
)

k_list = [RECOMMENDATION_NUMBER, NUMBER_OF_PRESELECTED_OFFERS]


def evaluate(
    experiment_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    )
):

    booking_raw_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="training_data_bookings"
    )
    training_item_categories = get_column_data(
        dataset=f"raw_{ENV_SHORT_NAME}",
        table_name="recommendation_training_data",
        column_name="item_id, offer_categoryId",
    )
    test_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="recommendation_test_data"
    )

    # We test maximum EVALUATION_USER_NUMBER users
    users_to_test = test_data["user_id"].unique()[
        : min(EVALUATION_USER_NUMBER, test_data["user_id"].nunique())
    ]
    test_data = test_data.loc[lambda df: df["user_id"].isin(users_to_test)]

    loaded_model = tf.keras.models.load_model(
        mlflow.get_artifact_uri("model"),
        custom_objects={"MatchModel": MatchModel},
        compile=False,
    )

    data_model_dict = {
        "name": experiment_name,
        "data": {
            "raw": booking_raw_data,
            "training_item_ids": training_item_categories["item_id"].unique(),
            "test": test_data,
        },
        "model": loaded_model,
    }

    # Compute metrics
    metrics = load_metrics(data_model_dict, k_list, RECOMMENDATION_NUMBER)

    # Connect to current MLFlow experiment
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id

    with mlflow.start_run(run_id=run_id) as run:
        artifact_uri = mlflow.get_artifact_uri("model")
        run_uuid = mlflow.active_run().info.run_uuid
        export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/"

        # Export metrics to MLFlow
        mlflow.log_metrics(metrics)

        # Export the PCA representations of the item embeddings
        pca_representations_path = export_path + "pca_plots/"
        save_pca_representation(
            loaded_model,
            training_item_categories,
            figures_folder=pca_representations_path,
        )
        mlflow.log_artifacts(pca_representations_path, "pca_plots")

        # Save the experiment information in BigQuery
        log_results = {
            "execution_date": datetime.now().isoformat(),
            "experiment_name": experiment_name,
            "model_name": experiment_name,
            "model_type": "tensorflow",
            "run_id": run_id,
            "run_start_time": run.info.start_time,
            "run_end_time": run.info.start_time,
            "artifact_uri": artifact_uri,
            "serving_container": SERVING_CONTAINER,
            "precision_at_10": metrics["precision_at_10"],
            "recall_at_10": metrics["recall_at_10"],
            "coverage_at_10": metrics["coverage_at_10"],
        }
        pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
            f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
            project_id=f"{GCP_PROJECT_ID}",
            if_exists="append",
        )

        remove_dir(export_path)
        print("------- EVALUATE DONE -------")


if __name__ == "__main__":
    typer.run(evaluate)
