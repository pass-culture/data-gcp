import pandas as pd
from datetime import datetime
import typer
from app.model import TrainPipeline
import mlflow
import os
from sklearn.model_selection import train_test_split
from utils import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    deploy_container,
    save_experiment,
    connect_remote_mlflow,
    get_mlflow_experiment,
    get_secret,
)
from figure import plot_features_importance, plot_cm, plot_hist


PARAMS = {"seen": 1_00_000, "consult": 1_00_000, "booking": 1_00_000}

MODEL_PARAMS = {
    "objective": "regression",
    "metric": {"l2", "l1"},
    "is_unbalance": True,
    "num_leaves": 31,
    "learning_rate": 0.05,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1,
}


def load_data(dataset_name, table_name):
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
    return pd.read_gbq(sql).sample(frac=1)


def plot_figures(test_data, train_data, pipeline, figure_folder):
    os.makedirs(figure_folder, exist_ok=True)

    for prefix, df in [("test_", test_data), ("train_", train_data)]:
        plot_hist(df, figure_folder, prefix=prefix)

        plot_cm(
            y=df["target"],
            y_pred=df["score"],
            filename=f"{figure_folder}/{prefix}confusion_matrix_perc_proba_0.5.pdf",
            perc=True,
            proba=0.5,
        )
        plot_cm(
            y=df["target"],
            y_pred=df["score"],
            filename=f"{figure_folder}/{prefix}confusion_matrix_total_proba_0.5.pdf",
            perc=False,
            proba=0.5,
        )
        plot_cm(
            y=df["target"],
            y_pred=df["score"],
            filename=f"{figure_folder}/{prefix}confusion_matrix_perc_proba_1.5.pdf",
            perc=True,
            proba=1.5,
        )
        plot_cm(
            y=df["target"],
            y_pred=df["score"],
            filename=f"{figure_folder}/{prefix}confusion_matrix_total_proba_1.5.pdf",
            perc=False,
            proba=1.5,
        )
    plot_features_importance(
        pipeline, filename=f"{figure_folder}/plot_features_importance.pdf"
    )


def train_pipeline(dataset_name, table_name, experiment_name, run_name):
    data = load_data(dataset_name, table_name)
    data["consult"] = data["consult"].astype(float).fillna(0)
    data["booking"] = data["booking"].astype(float).fillna(0)

    data["delta_diversification"] = (
        data["delta_diversification"].astype(float).fillna(0)
    )
    data["target"] = data["consult"] + data["booking"] * (
        1 + data["delta_diversification"]
    )
    train_data, test_data = train_test_split(data, test_size=0.2)

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    figure_folder = f"/tmp/{experiment_name}/"
    experiment = get_mlflow_experiment(experiment_name)

    mlflow.lightgbm.autolog()
    pipeline = TrainPipeline(target="target", verbose=True, params=MODEL_PARAMS)

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        pipeline.set_pipeline()
        pipeline.train(train_data)

        test_data = pipeline.predict(test_data)
        train_data = pipeline.predict(train_data)
        plot_figures(test_data, train_data, pipeline, figure_folder)

        mlflow.log_artifacts(figure_folder, "model_plots")

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
