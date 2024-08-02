import os
import shutil
from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
import typer
from sklearn.model_selection import train_test_split

from app.model import TrainPipeline
from figure import plot_cm, plot_cm_multiclass, plot_features_importance
from utils import (
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
MODEL_PARAMS = {
    "objective": "multiclass",
    "num_class": 3,
    "metric": "multi_logloss",
    "learning_rate": 0.05,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 5,
    "lambda_l2": 0.1,
    "lambda_l1": 0.1,
    "verbose": -1,
}
PROBA_CONSULT_RANGE = np.arange(0.4, 0.6, 0.05)
PROBA_BOOKING_RANGE = np.arange(0.4, 0.6, 0.05)
CLASS_MAPPING = {"seen": 0, "consult": 1, "booked": 2}


def load_data(dataset_name: str, table_name: str) -> pd.DataFrame:
    sql = f"""
    WITH seen AS (
      SELECT
          * 
      FROM `{GCP_PROJECT_ID}.{dataset_name}.{table_name}` 
      WHERE not consult and not booking
      ORDER BY offer_order ASC
      LIMIT {PARAMS['seen']}
    ),
    consult AS (
        SELECT
            * 
        FROM `{GCP_PROJECT_ID}.{dataset_name}.{table_name}` 
        WHERE consult
        LIMIT {PARAMS['consult']}

    ),
    booking AS (
      SELECT
            * 
        FROM `{GCP_PROJECT_ID}.{dataset_name}.{table_name}` 
        WHERE booking
        LIMIT {PARAMS['booking']}
    )
    
    SELECT * FROM seen 
    UNION ALL 
    SELECT * FROM consult 
    UNION ALL 
    select * FROM booking 
    """
    print(sql)
    return pd.read_gbq(sql, location="europe-west1").sample(frac=1)


def plot_figures(
    test_data: pd.DataFrame,
    train_data: pd.DataFrame,
    pipeline: TrainPipeline,
    figure_folder: str,
):
    shutil.rmtree(figure_folder, ignore_errors=True)
    os.makedirs(figure_folder)

    for prefix, df in [("test_", test_data), ("train_", train_data)]:
        for proba_consult in PROBA_CONSULT_RANGE:
            plot_cm(
                y=df["consult"],
                y_pred=df["prob_class_1"],
                filename=f"{figure_folder}/{prefix}cm_consult_proba_{proba_consult:.3f}.pdf",
                perc=True,
                proba=proba_consult,
            )
            for proba_booking in PROBA_BOOKING_RANGE:
                if proba_consult == PROBA_CONSULT_RANGE[0]:
                    plot_cm(
                        y=df["booking"],
                        y_pred=df["prob_class_2"],
                        filename=f"{figure_folder}/{prefix}cm_booking_proba_{proba_booking:.3f}.pdf",
                        perc=True,
                        proba=proba_booking,
                    )
                plot_cm_multiclass(
                    y_true_series=df["target_class"],
                    y_pred_consulted_series=df["prob_class_1"],
                    y_pred_booked_series=df["prob_class_2"],
                    perc_consulted=proba_consult,
                    perc_booked=proba_booking,
                    filename=f"{figure_folder}/{prefix}cm_multiclass_consult_{proba_consult:.3f}_booking_{proba_booking:.3f}.pdf",
                    class_names=["seen", "consult", "booked"],
                )

    plot_features_importance(
        pipeline, filename=f"{figure_folder}/plot_features_importance.pdf"
    )


def preprocess_data(data: pd.DataFrame, class_mapping: dict) -> pd.DataFrame:
    return (
        data.astype(
            {
                "consult": "float",
                "booking": "float",
                "delta_diversification": "float",
            }
        )
        .fillna({"consult": 0, "booking": 0, "delta_diversification": 0})
        .assign(
            status=lambda df: pd.Series(["seen"] * len(df))
            .where(df["consult"] != 1.0, other="consult")
            .where(df["booking"] != 1.0, other="booked"),
            target_class=lambda df: df["status"].map(class_mapping).astype(int),
        )
    ).drop_duplicates()


def train_pipeline(dataset_name, table_name, experiment_name, run_name):
    # data = load_data(dataset_name, table_name)
    # data.to_csv("data.csv", index=False)

    # Load and preprocess the data
    data = pd.read_csv("data.csv")
    preprocessed_data = data.pipe(preprocess_data, class_mapping=CLASS_MAPPING)
    train_data, test_data = train_test_split(preprocessed_data, test_size=TEST_SIZE)

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment = get_mlflow_experiment(experiment_name)
    figure_folder = f"/tmp/{experiment_name}/"

    # Start training
    mlflow.lightgbm.autolog()
    pipeline = TrainPipeline(target="target_class", params=MODEL_PARAMS)

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        pipeline.set_pipeline()
        pipeline.train(train_data)

        predictions_on_test_data = pipeline.predict(test_data)
        predictions_on_train_data = pipeline.predict(train_data)

        # Save Data
        plot_figures(
            predictions_on_test_data, predictions_on_train_data, pipeline, figure_folder
        )
        predictions_on_train_data.to_csv(
            f"{figure_folder}/train_predictions.csv", index=False
        )
        predictions_on_test_data.to_csv(
            f"{figure_folder}/test_predictions.csv", index=False
        )
        mlflow.log_artifacts(figure_folder, "model_plots_and_predictions")

    # retrain on whole
    pipeline.train(data)
    # save
    pipeline.save()


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
    serving_container = (
        f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    )
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
