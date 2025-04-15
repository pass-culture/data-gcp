import os
import secrets
import shutil
from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
import typer
from sklearn.model_selection import train_test_split

from app.model import (
    ClassMapping,
    TrainPipeline,
)
from src.evaluation import compute_ndcg_at_k
from src.figure import (
    plot_cm,
    plot_cm_multiclass,
    plot_features_importance,
)
from src.preprocessing import map_features_columns, preprocess_data
from src.utils import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    connect_remote_mlflow,
    deploy_container,
    get_mlflow_experiment,
    get_secret,
    save_experiment,
)

PARAMS = {"seen": 500_000, "consult": 500_000, "booking": 500_000}
TEST_SIZE = 0.1
CLASSIFIER_MODEL_PARAMS = {
    "objective": "multiclass",
    "num_class": 3,
    "metric": "multi_logloss",
    "learning_rate": 0.03,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 5,
    "lambda_l2": 1,
    "lambda_l1": 1,
    "verbose": -1,
    "num_leaves": 10,
}
PROBA_CONSULT_THRESHOLD = 0.5
PROBA_BOOKING_THRESHOLD = 0.5
NDCG_K_LIST = [5, 10, 20]


def load_data(dataset_name: str, table_name: str) -> pd.DataFrame:
    # Hack : Use new training data
    dataset_name = f"ml_reco_{ENV_SHORT_NAME}"
    table_name = "ranking_training_data"
    sql = f"""
    select * from `{GCP_PROJECT_ID}.{dataset_name}.{table_name}`
    """
    # End Hack
    print(sql)

    return pd.read_gbq(sql).sample(frac=1).drop_duplicates()


def plot_figures(
    test_data: pd.DataFrame,
    train_data: pd.DataFrame,
    pipeline: TrainPipeline,
    figure_folder: str,
):
    shutil.rmtree(figure_folder, ignore_errors=True)
    os.makedirs(figure_folder)

    for prefix, df in [("train_", train_data), ("test_", test_data)]:
        print(f"Plotting figures for {prefix} data")
        plot_cm(
            y=df[ClassMapping.consulted.name],
            y_pred=df["predicted_class"] == ClassMapping.consulted.value,
            filename=f"{figure_folder}/{prefix}cm_{ClassMapping.consulted.name}.pdf",
            perc=True,
        )
        plot_cm(
            y=df[ClassMapping.booked.name],
            y_pred=df["predicted_class"] == ClassMapping.booked.value,
            filename=f"{figure_folder}/{prefix}cm_{ClassMapping.booked.name}.pdf",
            perc=True,
        )
        plot_cm_multiclass(
            y_true=df["target_class"],
            y_pred=df["predicted_class"],
            filename=f"{figure_folder}/{prefix}cm_multiclass.pdf",
            class_names=[class_mapping.name for class_mapping in ClassMapping],
        )

    plot_features_importance(
        pipeline, filename=f"{figure_folder}/plot_features_importance.pdf"
    )


def train_pipeline(dataset_name, table_name, experiment_name, run_name):
    # Load and preprocess the data
    raw_data = load_data(dataset_name, table_name).pipe(map_features_columns)
    preprocessed_data = raw_data.pipe(preprocess_data)

    # Split based on unique_session_id
    seed = secrets.randbelow(1000)
    unique_user_x_date_ids = preprocessed_data.user_x_date_id.unique()
    train_session_ids, test_session_ids = train_test_split(
        unique_user_x_date_ids, test_size=TEST_SIZE, random_state=seed
    )
    train_data = preprocessed_data[
        preprocessed_data["user_x_date_id"].isin(train_session_ids)
    ]
    test_data = preprocessed_data[
        preprocessed_data["user_x_date_id"].isin(test_session_ids)
    ]

    # Compute class weights
    class_frequency = train_data.target_class.value_counts(normalize=True).to_dict()
    class_weight = {k: 1 / v for k, v in class_frequency.items()}

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment = get_mlflow_experiment(experiment_name)
    figure_folder = f"/tmp/{experiment_name}/"

    # Start training
    mlflow.lightgbm.autolog()
    pipeline_classifier = TrainPipeline(
        target="target_class", params=CLASSIFIER_MODEL_PARAMS
    )

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        pipeline_classifier.set_pipeline()
        print("Training model...")
        pipeline_classifier.train(train_data, class_weight=class_weight, seed=seed)
        print("Training finished")

        print("Evaluating model...")
        train_predictions = train_data.pipe(pipeline_classifier.predict_classifier)
        test_predictions = test_data.pipe(pipeline_classifier.predict_classifier)
        ndcg_at_k = compute_ndcg_at_k(predictions=test_predictions, k_list=NDCG_K_LIST)
        random_ndcg_at_k = compute_ndcg_at_k(
            predictions=test_predictions.assign(
                score=np.random.rand(len(test_predictions))
            ),
            k_list=NDCG_K_LIST,
        )
        for k in NDCG_K_LIST:
            mlflow.log_metric(f"ndcg_at_{k}", ndcg_at_k[k])
            mlflow.log_metric(f"random_ndcg_at_{k}", random_ndcg_at_k[k])
        print("Evaluation finished")

        # Save Data
        print("Plotting Figures...")
        plot_figures(
            train_data=train_predictions,
            test_data=test_predictions,
            pipeline=pipeline_classifier,
            figure_folder=figure_folder,
        )
        print("Figures plotted")

        print("Saving Data...")
        train_predictions.to_parquet(
            f"{figure_folder}/train_predictions.parquet", index=False
        )
        test_predictions.to_parquet(
            f"{figure_folder}/test_predictions.parquet", index=False
        )
        print("Data saved")

        mlflow.log_artifacts(figure_folder, "model_plots_and_predictions")
        mlflow.log_param("seed", seed)

    # # retrain on whole
    # pipeline_classifier.train(preprocessed_data, class_weight=class_weight)

    # save
    pipeline_classifier.save(model_name="classifier")


def main(
    experiment_name: str = typer.Option(
        None,
        help="Name of the experiment",
    ),
    run_name: str = typer.Option(
        None,
        help="Name of the run",
    ),
    model_name: str = typer.Option(
        None,
        help="Name of the model",
    ),
    dataset_name: str = typer.Option(
        None,
        help="Name input dataset with preproc data",
    ),
    table_name: str = typer.Option(
        None,
        help="Name input table with preproc data",
    ),
) -> None:
    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/ranking-endpoint/{ENV_SHORT_NAME}/{experiment_name.replace('.', '_')}:{run_id}"

    train_pipeline(
        dataset_name=dataset_name,
        table_name=table_name,
        experiment_name=experiment_name,
        run_name=run_name,
    )
    print("Deploy...")
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
