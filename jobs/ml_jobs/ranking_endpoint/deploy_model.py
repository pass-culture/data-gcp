import os
import shutil
from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
import typer

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
from src.preprocessing import (
    map_features_columns,
    preprocess_data,
)
from src.utils import (
    ENV_SHORT_NAME,
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
    "feature_fraction": 0.8,  # Will use 80% of features for each tree
    "bagging_fraction": 0.8,  # Will use 80% of data for each tree
    "bagging_freq": 1,  # Perform bagging at every iteration
    "lambda_l2": 50,
    "lambda_l1": 50,
    "verbose": -1,
    "num_leaves": 10,
}

# Parameters for LambdaRank learning to rank
LAMBDARANK_MODEL_PARAMS = {
    "objective": "lambdarank",  # Learning to rank objective
    "metric": "ndcg",  # Use NDCG as the evaluation metric
    "ndcg_at": [5, 10, 20],  # Calculate NDCG at these positions
    "learning_rate": 0.05,
    "num_leaves": 31,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "lambda_l1": 0.1,
    "lambda_l2": 0.1,
    "max_depth": -1,
    "min_data_in_leaf": 20,
    "verbose": -1,
}

NDCG_K_LIST = [5, 10, 20]


def load_data(input_ccs_dir: str) -> pd.DataFrame:
    return pd.read_parquet(input_ccs_dir).sample(frac=1).drop_duplicates()


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


def evaluate_model(
    train_predictions: pd.DataFrame,
    test_predictions: pd.DataFrame,
    pipeline_classifier: TrainPipeline,
    figure_folder: str,
    suffix: str,
) -> None:
    figure_folder = f"{figure_folder}{suffix}"

    print("Postprocessing predictions...")
    test_stats_df = pd.DataFrame.from_records(
        test_predictions.groupby("user_id").apply(
            lambda g: {
                "user_id": g.name,
                "item_count": g.item_id.nunique(),
            }
        )
    )
    user_ids_with_enough_items = test_stats_df.loc[
        lambda df: df.item_count >= 20, "user_id"
    ].drop_duplicates()
    test_predictions = test_predictions.loc[
        lambda df: df.user_id.isin(user_ids_with_enough_items)
    ].reset_index(drop=True)

    print(f"Number of users in test set : {len(test_stats_df)}")
    print(f"Number of users : {len(user_ids_with_enough_items)} with enough items")
    print(f"Postprocessing done, {len(test_predictions)} predictions")

    print("Evaluating model...")
    ndcg_at_k = compute_ndcg_at_k(predictions=test_predictions, k_list=NDCG_K_LIST)
    random_ndcg_at_k = compute_ndcg_at_k(
        predictions=test_predictions.assign(
            score=np.random.rand(len(test_predictions))
        ),
        k_list=NDCG_K_LIST,
    )
    popular_ndcg_at_k = compute_ndcg_at_k(
        predictions=test_predictions.assign(
            score=test_predictions["offer_booking_number_last_28_days"]
            / test_predictions["offer_booking_number_last_28_days"].max()
        ),
        k_list=NDCG_K_LIST,
    )
    for k in NDCG_K_LIST:
        mlflow.log_metric(f"ndcg{suffix}_at_{k}", ndcg_at_k[k])
        mlflow.log_metric(f"random_ndcg{suffix}_at_{k}", random_ndcg_at_k[k])
        mlflow.log_metric(f"popular_ndcg{suffix}_at_{k}", popular_ndcg_at_k[k])
    print("Evaluation finished")

    print("Saving Data...")
    os.makedirs(figure_folder, exist_ok=True)
    train_predictions.to_parquet(
        f"{figure_folder}/train_predictions.parquet", index=False
    )
    test_predictions.to_parquet(
        f"{figure_folder}/test_predictions.parquet", index=False
    )
    print("Data saved")

    mlflow.log_artifacts(figure_folder, f"model_plots_and_predictions{suffix}")


def train_pipeline(input_gcs_dir: str, experiment_name: str, run_name: str) -> None:
    """
    Train a LightGBM ranking model pipeline with MLflow experiment tracking.
    This function performs end-to-end model training including data loading, preprocessing,
    train-test splitting, model training with class weights, evaluation using NDCG metrics,
    and artifact logging to MLflow.
    Args:
        input_gcs_dir (str): Path to the Google Cloud Storage directory containing input data.
        experiment_name (str): Name of the MLflow experiment for tracking this training run.
        run_name (str): Specific name for this training run within the experiment.
    Returns:
        None
    Process:
        1. Loads and preprocesses data from GCS
        2. Splits data by event_date into train/test sets
        3. Computes class weights based on target class frequency
        4. Connects to remote MLflow for experiment tracking
        5. Trains LightGBM classifier with auto-logging enabled
        6. Evaluates model using NDCG@k metrics against random and popularity baselines
        7. Generates and saves visualization plots
        8. Logs all metrics, parameters, and artifacts to MLflow
        9. Saves the trained pipeline model
    Note:
        - Uses a random seed for reproducible train-test splits
        - Computes NDCG metrics for multiple k values defined in NDCG_K_LIST
        - Saves predictions, plots, and model artifacts to MLflow
        - Model is saved with the name "classifier"
    """

    # Load and preprocess the data
    raw_data = load_data(input_gcs_dir).pipe(
        map_features_columns,
    )
    preprocessed_data = raw_data.pipe(preprocess_data)

    # Split based on unique_session_id
    train_data, test_data = TrainPipeline.linear_train_test_split(
        data_to_split_df=preprocessed_data,
        split_key="event_date",
        test_size=TEST_SIZE,
    )

    # Compute class weights
    class_frequency = train_data.target_class.value_counts(normalize=True).to_dict()
    class_weight = {k: 1 / v for k, v in class_frequency.items()}

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment = get_mlflow_experiment(experiment_name)

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

        print("Whole data evaluation...")
        train_predictions = train_data.pipe(pipeline_classifier.predict_classifier)
        test_predictions = test_data.pipe(pipeline_classifier.predict_classifier)
        evaluate_model(
            train_predictions=train_predictions,
            test_predictions=test_predictions,
            pipeline_classifier=pipeline_classifier,
            figure_folder=f"/tmp/{experiment_name}",
            suffix="",
        )

        print("Evaluation on recommendation only...")
        train_recommendation_predictions = train_data.loc[
            lambda df: df.context == "recommendation"
        ].pipe(pipeline_classifier.predict_classifier)
        test_recommendation_predictions = test_data.loc[
            lambda df: df.context == "recommendation"
        ].pipe(pipeline_classifier.predict_classifier)
        evaluate_model(
            train_predictions=train_recommendation_predictions,
            test_predictions=test_recommendation_predictions,
            pipeline_classifier=pipeline_classifier,
            figure_folder=f"/tmp/{experiment_name}",
            suffix="_reco",
        )

    # TODO: Check to see if it is better with or without
    # # retrain on whole
    # pipeline_classifier.train(preprocessed_data, class_weight=class_weight)

    # save
    pipeline_classifier.save(model_name="classifier")


def preprocess_for_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess data for learning to rank by converting class labels to relevance scores.

    Learning to rank requires numerical relevance labels where higher values indicate
    more relevant items. This function converts class labels to relevance scores:
    - booked (most relevant) = 2
    - consulted/clicked = 1
    - seen (least relevant) = 0

    Args:
        df (pd.DataFrame): Input dataframe with class labels

    Returns:
        pd.DataFrame: Processed dataframe with relevance scores
    """
    # Create a relevance column for ranking (higher = more relevant)
    return df.assign(
        relevance=lambda x: (
            (x[ClassMapping.consulted.name] * 1) + (x[ClassMapping.booked.name] * 2)
        )
    )


def train_ranking_pipeline(
    input_gcs_dir: str, experiment_name: str, run_name: str
) -> None:
    """
    Train a LightGBM LambdaRank learning-to-rank model with MLflow experiment tracking.

    This function performs end-to-end model training for a learning-to-rank approach:
    1. Loads and preprocesses data from GCS
    2. Converts classification labels to relevance scores
    3. Uses LambdaRank objective with grouped data by user_id
    4. Evaluates using NDCG metrics
    5. Logs all metrics, parameters, and artifacts to MLflow

    Args:
        input_gcs_dir (str): Path to the Google Cloud Storage directory containing input data.
        experiment_name (str): Name of the MLflow experiment for tracking this training run.
        run_name (str): Specific name for this training run within the experiment.

    Returns:
        None
    """
    # Load and preprocess the data
    raw_data = load_data(input_gcs_dir).pipe(
        map_features_columns,
    )
    preprocessed_data = raw_data.pipe(preprocess_data)

    # Preprocess for learning to rank by creating relevance scores
    ranking_data = preprocess_for_ranking(preprocessed_data)

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment = get_mlflow_experiment(experiment_name)

    # Start training
    mlflow.lightgbm.autolog()
    pipeline_ranking = TrainPipeline(target="relevance", params=LAMBDARANK_MODEL_PARAMS)

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        # Log parameters specific to learning to rank
        mlflow.log_params(
            {
                "model_type": "learning_to_rank",
                "ranking_algorithm": "lambdarank",
                "group_key": "user_id",
                "relevance_levels": 3,  # 0=seen, 1=consulted, 2=booked
            }
        )

        pipeline_ranking.set_pipeline()
        print("Training ranking model...")
        pipeline_ranking.train_ranking(ranking_data)
        print("Training finished")

        # Generate predictions
        train_data, test_data = pipeline_ranking.linear_train_test_split(
            data_to_split_df=ranking_data,
            split_key="event_date",
            test_size=TEST_SIZE,
        )

        train_predictions = train_data.pipe(pipeline_ranking.predict_ranking)
        test_predictions = test_data.pipe(pipeline_ranking.predict_ranking)

        # Evaluate
        print("Evaluating model...")
        evaluate_model(
            train_predictions=train_predictions,
            test_predictions=test_predictions,
            pipeline_classifier=pipeline_ranking,  # Reuse the evaluation function
            figure_folder=f"/tmp/{experiment_name}",
            suffix="_lambdarank",
        )

        # Evaluate on recommendation context only
        train_recommendation_predictions = train_data.loc[
            lambda df: df.context == "recommendation"
        ].pipe(pipeline_ranking.predict_ranking)
        test_recommendation_predictions = test_data.loc[
            lambda df: df.context == "recommendation"
        ].pipe(pipeline_ranking.predict_ranking)

        evaluate_model(
            train_predictions=train_recommendation_predictions,
            test_predictions=test_recommendation_predictions,
            pipeline_classifier=pipeline_ranking,
            figure_folder=f"/tmp/{experiment_name}",
            suffix="_lambdarank_reco",
        )

    # Save the model
    pipeline_ranking.save(model_name="lambdarank")


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
    input_gcs_dir: str = typer.Option(
        None,
        help="GCS directory where the input data is stored",
    ),
    training_only: bool = typer.Option(
        False,
        help="If True, only train the model without deploying",
    ),
    use_lambdarank: bool = typer.Option(
        True,
        help="If True, use LambdaRank learning to rank instead of classification",
    ),
) -> None:
    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/ranking-endpoint/{ENV_SHORT_NAME}/{experiment_name.replace('.', '_')}:{run_id}"

    if use_lambdarank:
        train_ranking_pipeline(
            input_gcs_dir=input_gcs_dir,
            experiment_name=experiment_name,
            run_name=run_name,
        )
    else:
        train_pipeline(
            input_gcs_dir=input_gcs_dir,
            experiment_name=experiment_name,
            run_name=run_name,
        )

    if training_only:
        print("Training only, skipping deployment...")
        return

    print("Deploy...")
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
