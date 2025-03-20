from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import tensorflow as tf
from loguru import logger
from recommenders.evaluation.python_evaluation import (
    catalog_coverage,
    novelty,
    precision_at_k,
    recall_at_k,
)
from tqdm import tqdm

from commons.constants import EVALUATION_USER_NUMBER
from commons.data_collect_queries import read_from_gcs
from two_towers_model.utils.evaluation.evaluation_baselines import (
    generate_popularity_baseline,
    generate_random_baseline,
)


def load_data_for_evaluation(
    storage_path: str,
    train_dataset_name: str,
    test_dataset_name: str,
) -> Dict[str, pd.DataFrame]:
    """
    Loads train and test datasets for evaluation by reading data from storage,
    keeping only necessary columns and removing duplicates.

    Args:
        storage_path (str): Path to the storage location (e.g., GCS bucket path).
        train_dataset_name (str): Name of the train dataset file.
        test_dataset_name (str): Name of the test dataset file.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary with keys 'train' and 'test' containing the datasets.
    """

    list_columns_to_keep = ["user_id", "item_id"]

    logger.info("Load train")
    train_data = read_from_gcs(storage_path, train_dataset_name, parallel=False)[
        list_columns_to_keep
    ].drop_duplicates()
    logger.info(
        f"Number of unique (user, item) interactions in train data: {train_data.shape[0]}"
    )

    logger.info("Load test data")
    test_data = (
        read_from_gcs(storage_path, test_dataset_name, parallel=False)
        .astype({"user_id": str, "item_id": str})[list_columns_to_keep]
        .drop_duplicates()
    )
    logger.info(
        f"Number of unique (user, item, ) interactions in test data: {test_data.shape[0]}"
    )

    return {"train": train_data, "test": test_data}


def generate_predictions(
    model,
    test_data: pd.DataFrame,
    training_data: pd.DataFrame,
    all_users: bool,
    batch_size: int = 4000,  # Optimized for 24GB RAM: 4000 users × 100k items × 4MB ≈ 16GB
    top_k: int = 1100,  # 1000 items + 100 items to hedge against filtering items seen during training
) -> pd.DataFrame:
    """
    Generates item dot product scores for each user in the test dataset using the provided model.
    The scores are computed for each user with all unique (item_id) in test_dataset which are stored in offers_to_score.
    After prediction, removes items that users have already interacted with in training data.
    Returns only top-k items per user after filtering.

    Args:
        model: Trained Two tower retrieval model.
        test_data (pd.DataFrame): DataFrame containing test user-item interactions with at least
            'user_id' and 'item_id' columns.
        training_data (pd.DataFrame): DataFrame containing training user-item interactions.
        all_users (bool): If True, evaluate all users in the test dataset. If False, evaluate only a subset of users.
        batch_size (int): Number of users to process in each batch. Defaults to 4000, optimized for 24GB RAM.
        top_k (int): Number of top items to keep per user after filtering. Defaults to 1100 (1000 items + 100 items to hedge against filtering items seen during training).

    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], containing top-k items per user
                     after removing training interactions.
    """
    # Get unique items to score before filtering users
    offers_to_score = test_data.item_id.unique()

    # Filter users if needed
    total_n_users = len(test_data["user_id"].unique())
    if not all_users:
        users_to_test = test_data["user_id"].unique()[
            : min(EVALUATION_USER_NUMBER, total_n_users)
        ]
        test_data = test_data[test_data.user_id.isin(users_to_test)]
        logger.info(f"Computing metrics for {len(users_to_test)} users")
    else:
        logger.info(f"Computing metrics for all users ({total_n_users} users)")

    # Process users in batches
    list_df_predictions = []
    unique_users = test_data["user_id"].unique()

    for i in tqdm(range(0, len(unique_users), batch_size)):
        batch_users = unique_users[i : i + batch_size]

        # Prepare input for batch prediction
        user_input = np.repeat(batch_users, len(offers_to_score))
        item_input = np.tile(offers_to_score, len(batch_users))

        # Generate predictions for the batch
        prediction_input = [user_input, item_input]
        predictions = model.predict(prediction_input, verbose=0)

        # Create DataFrame for this batch
        batch_predictions = pd.DataFrame(
            {
                "user_id": user_input,
                "item_id": item_input,
                "score": predictions.flatten(),
            }
        )

        # Keep top-k items per user
        batch_predictions = batch_predictions.sort_values("score", ascending=False)
        batch_predictions = batch_predictions.groupby("user_id").head(top_k)

        list_df_predictions.append(batch_predictions)

    # Combine all batches
    df_predictions = pd.concat(list_df_predictions, ignore_index=True)

    # Filter out items that users have already interacted with in training data by performing an anti-join
    df_predictions = df_predictions.merge(
        training_data, on=["user_id", "item_id"], how="left", indicator=True
    )
    df_predictions = df_predictions[df_predictions["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )

    return df_predictions


def compute_metrics(
    test_data: pd.DataFrame,
    df_predictions: pd.DataFrame,
    train_data: pd.DataFrame,
    k: int,
    prefix: Optional[str] = "",
) -> Dict[str, float]:
    """
    Computes evaluation metrics at top-k: precision, recall, coverage, and novelty.

    Args:
        test_data (pd.DataFrame): Ground truth user-item interactions (columns: 'user_id', 'item_id').
        df_predictions (pd.DataFrame): Predicted scores for user-item pairs
                                       (columns: 'user_id', 'item_id', 'score').
        train_data (pd.DataFrame): Historical train interactions (columns: 'user_id', 'item_id').
        k (int): Cutoff for top-k retrieval.
        prefix (str, optional): Prefix for metric keys in output dictionary. Defaults to "".

    Returns:
        Dict[str, float]: Dictionary containing:
            - {prefix}precision_at_k
            - {prefix}recall_at_k
            - {prefix}coverage_at_k
            - {prefix}novelty_at_k
    """

    # Column definitions
    col_user = "user_id"
    col_item = "item_id"
    col_prediction = "score"

    # Calculate metrics
    precision = precision_at_k(
        test_data,
        df_predictions,
        col_user=col_user,
        col_item=col_item,
        col_prediction=col_prediction,
        relevancy_method="top_k",
        k=k,
    )

    recall = recall_at_k(
        test_data,
        df_predictions,
        col_user=col_user,
        col_item=col_item,
        col_prediction=col_prediction,
        relevancy_method="top_k",
        k=k,
    )

    coverage = catalog_coverage(
        train_data, df_predictions, col_user=col_user, col_item=col_item
    )

    novelty_metric = novelty(
        train_data, df_predictions, col_user=col_user, col_item=col_item
    )

    ## Assemble metrics in dict
    metrics = {
        f"{prefix}precision_at_{k}": precision,
        f"{prefix}recall_at_{k}": recall,
        f"{prefix}novelty_at_{k}": novelty_metric,
        f"{prefix}coverage_at_{k}": coverage,
    }
    return metrics


def evaluate(
    model: tf.keras.models.Model,
    storage_path: str,
    train_dataset_name: str,
    test_dataset_name: str,
    list_k: List[int],
    all_users: bool,
    dummy: bool,
) -> Dict[str, float]:
    """
    Runs the full evaluation pipeline for a retrieval model, including:
    - Data loading
    - Prediction generation
    - Metric computation (precision, recall, novelty, and coverage)

    Optionally compares against random and popularity-based baselines.

    Args:
        model (tf.keras.models.Model): Trained two-tower retrieval model.
        storage_path (str): Path to data storage.
        train_dataset_name (str): Filename of train dataset in storage.
        test_dataset_name (str): Filename of test dataset in storage.
        list_k (List[int]): List of k values (top-k cutoff) for metrics evaluation.
        all_users (bool): Whether to evaluate all users or a subset.
        dummy (bool): Whether to also compute metrics for random and popularity baselines.

    Returns:
        Dict[str, float]: Dictionary containing evaluation metrics for each k value.
    """

    logger.info("Getting data for evaluation")
    data_dict = load_data_for_evaluation(
        storage_path=storage_path,
        train_dataset_name=train_dataset_name,
        test_dataset_name=test_dataset_name,
    )

    list_predictions_to_evaluate = []
    logger.info("Inferring model predictions")
    df_predictions = generate_predictions(
        model=model, test_data=data_dict["test"], all_users=all_users
    )
    list_predictions_to_evaluate.append({"predictions": df_predictions, "prefix": ""})

    if dummy:
        # Extract unique users from model predictions to ensure consistent user set
        predicted_users = df_predictions["user_id"].unique().tolist()
        logger.info(f"Using {len(predicted_users)} users for baseline evaluations")

        logger.info("Generating random baseline predictions")
        df_random = generate_random_baseline(
            test_data=data_dict["test"],
            num_recommendations=max(list_k),
            specific_users=predicted_users,
        )
        list_predictions_to_evaluate.append(
            {"predictions": df_random, "prefix": "random_"}
        )

        logger.info("Generating popularity baseline predictions")
        df_popular = generate_popularity_baseline(
            test_data=data_dict["test"],
            num_recommendations=max(list_k),
            specific_users=predicted_users,
        )
        list_predictions_to_evaluate.append(
            {"predictions": df_popular, "prefix": "popular_"}
        )

    logger.info("Computing metrics")
    metrics = {}

    for predictions in list_predictions_to_evaluate:
        for k in list_k:
            metrics.update(
                compute_metrics(
                    test_data=data_dict["test"],
                    df_predictions=predictions["predictions"],
                    train_data=data_dict["train"],
                    k=k,
                    prefix=predictions["prefix"],
                )
            )

    return metrics
