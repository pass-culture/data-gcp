import glob
import json
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
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    ClassMapping,
    TrainPipeline,
)
from figure import (
    plot_cm,
    plot_cm_multiclass,
    plot_features_importance,
)
from preprocessing import map_features_columns
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
CLASSIFIER_MODEL_PARAMS = {
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
    "num_leaves": 41,
}
PROBA_CONSULT_THRESHOLD = 0.5
PROBA_BOOKING_THRESHOLD = 0.5


def load_data(dataset_name: str, table_name: str) -> pd.DataFrame:
    return pd.concat(
        [pd.read_parquet(f) for f in glob.glob("data_all/data*.parquet")],
        ignore_index=True,
    )

    sql = f"""
    WITH seen AS (
      SELECT
          *
      FROM `{GCP_PROJECT_ID}.{dataset_name}.{table_name}`
      WHERE not consult and not booking
      ORDER BY offer_order ASC
      LIMIT {PARAMS["seen"]}
    ),
    consult AS (
        SELECT
            *
        FROM `{GCP_PROJECT_ID}.{dataset_name}.{table_name}`
        WHERE consult and not booking
        LIMIT {PARAMS["consult"]}

    ),
    booking AS (
      SELECT
            *
        FROM `{GCP_PROJECT_ID}.{dataset_name}.{table_name}`
        WHERE booking
        LIMIT {PARAMS["booking"]}
    )
    SELECT * FROM seen
    UNION ALL
    SELECT * FROM consult
    UNION ALL
    SELECT * FROM booking
    """
    print(sql)

    data = pd.read_gbq(sql).sample(frac=1)
    n_rows_duplicated = data.duplicated().sum()
    if n_rows_duplicated > 0:
        raise ValueError(
            f"Duplicated rows in data ({n_rows_duplicated} rows duplicated). Please review your SQL query."
        )

    return data


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
            y_pred=df[f"prob_class_{ClassMapping.consulted.name}"],
            filename=f"{figure_folder}/{prefix}cm_{ClassMapping.consulted.name}_proba_{PROBA_CONSULT_THRESHOLD:.3f}.pdf",
            perc=True,
            proba=PROBA_CONSULT_THRESHOLD,
        )
        plot_cm(
            y=df[ClassMapping.booked.name],
            y_pred=df[f"prob_class_{ClassMapping.booked.name}"],
            filename=f"{figure_folder}/{prefix}cm_{ClassMapping.booked.name}_proba_{PROBA_BOOKING_THRESHOLD:.3f}.pdf",
            perc=True,
            proba=PROBA_BOOKING_THRESHOLD,
        )
        plot_cm_multiclass(
            y_true=df["target_class"],
            y_pred_consulted=df[f"prob_class_{ClassMapping.consulted.name}"],
            y_pred_booked=df[f"prob_class_{ClassMapping.booked.name}"],
            perc_consulted=PROBA_CONSULT_THRESHOLD,
            perc_booked=PROBA_BOOKING_THRESHOLD,
            filename=f"{figure_folder}/{prefix}cm_multiclass_{ClassMapping.consulted.name}_{PROBA_CONSULT_THRESHOLD:.3f}_{ClassMapping.booked.name}_{PROBA_BOOKING_THRESHOLD:.3f}.pdf",
            class_names=[class_mapping.name for class_mapping in ClassMapping],
        )

    plot_features_importance(
        pipeline, filename=f"{figure_folder}/plot_features_importance.pdf"
    )


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    # Add features horizontally
    user_embed_dim = 64
    item_embed_dim = 64
    missing_array = json.dumps(np.array([0] * user_embed_dim).tolist())

    df = (
        (
            data.loc[
                :,
                lambda df: df.columns.isin(
                    ["is_seen", "is_consulted", "is_booked", "unique_session_id"]
                    + NUMERIC_FEATURES
                    + CATEGORICAL_FEATURES
                    + ["user_embedding_json", "item_embedding_json"]
                ),
            ]
            .astype(
                {
                    "is_consulted": "float",
                    "is_booked": "float",
                }
            )
            .fillna({"is_consulted": 0, "is_booked": 0})
            .rename(
                columns={
                    "is_consulted": ClassMapping.consulted.name,
                    "is_booked": ClassMapping.booked.name,
                }
            )
            .assign(
                status=lambda df: pd.Series([ClassMapping.seen.name] * len(df))
                .where(
                    df[ClassMapping.consulted.name] != 1.0,
                    other=ClassMapping.consulted.name,
                )
                .where(
                    df[ClassMapping.booked.name] != 1.0, other=ClassMapping.booked.name
                ),
                target_class=lambda df: df["status"]
                .map(
                    {
                        class_mapping.name: class_mapping.value
                        for class_mapping in ClassMapping
                    }
                )
                .astype(int),
            )
        )
        .drop_duplicates()
        .assign(
            user_embedding=lambda df: df.user_embedding_json.fillna(missing_array)
            .replace("null", missing_array)
            .apply(lambda x: np.array(json.loads(x))),
            item_embedding=lambda df: df.item_embedding_json.fillna(missing_array)
            .replace("null", missing_array)
            .apply(lambda x: np.array(json.loads(x))),
        )
    )

    print(df.head())

    # Stack arrays into 2D NumPy arrays
    user_embeddings_array = np.stack(df["user_embedding"].values)
    item_embeddings_array = np.stack(df["item_embedding"].values)

    # Convert to DataFrames with proper indices and column names
    user_embedding_features = pd.DataFrame(
        user_embeddings_array,
        index=df.index,
        columns=[f"user_emb_{i}" for i in range(user_embed_dim)],
    )
    item_embedding_features = pd.DataFrame(
        item_embeddings_array,
        index=df.index,
        columns=[f"item_emb_{i}" for i in range(item_embed_dim)],
    )

    # --- Optional but Recommended: Add Dot Product ---
    # This is faster using the NumPy arrays directly
    dot_product = np.sum(user_embeddings_array * item_embeddings_array, axis=1)
    df["embedding_dot_product"] = dot_product
    # ---------------------------------------------

    # Concatenate as before
    original_features = df.drop(
        columns=[
            "user_embedding_json",  # if you had it
            "item_embedding_json",  # if you had it
            "user_embedding",
            "item_embedding",
        ]
    )

    df_final_features = pd.concat(
        [original_features, user_embedding_features, item_embedding_features], axis=1
    )

    print(df_final_features.head())
    print(df_final_features.columns)
    return df_final_features


def train_pipeline(dataset_name, table_name, experiment_name, run_name):
    # Load and preprocess the data
    data = load_data(dataset_name, table_name).pipe(map_features_columns)
    preprocessed_data = data.pipe(
        preprocess_data,
    )

    seed = secrets.randbelow(1000)

    # Split based on unique_session_id

    # Filter data by session IDs
    unique_session_ids = preprocessed_data["unique_session_id"].unique()
    train_session_ids, test_session_ids = train_test_split(
        unique_session_ids, test_size=TEST_SIZE, random_state=seed
    )
    train_data = preprocessed_data[
        preprocessed_data["unique_session_id"].isin(train_session_ids)
    ]
    test_data = preprocessed_data[
        preprocessed_data["unique_session_id"].isin(test_session_ids)
    ]

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
        pipeline_classifier.train(train_data, class_weight=class_weight)
        print("Training finished")

        print("Evaluating model...")
        train_predictions = train_data.pipe(pipeline_classifier.predict_classifier)
        test_predictions = test_data.pipe(pipeline_classifier.predict_classifier)
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
