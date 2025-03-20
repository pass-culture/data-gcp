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
from two_towers_model.utils.evaluation_baselines import (
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
    model, test_data: pd.DataFrame, all_users: bool
) -> pd.DataFrame:
    """
    Generates item dot product scores for each user in the test dataset using the provided model.
    The scores are computed for each user with all unique (item_id) in test_dataset which are stored in offers_to_score.
    offers_to_score does not contain previously seen items during train which prevents data leak.

    Args:
        model: Trained Two tower retrieval model.
        test_data (pd.DataFrame): DataFrame containing test user-item interactions with at least
            'user_id' and 'item_id' columns.
        all_users : bool: If True, evaluate all users in the test dataset. If False, evaluate only a subset of users.

    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], where score represents
                        the predicted interaction score (dot product between user and item two tower embeddings).
    """
    ## TODO UNCOMMENT THIS AFTER BATCHING IS DONE
    ## Get unique items to score
    # offers_to_score = test_data.item_id.unique()
    # logger.info(f"Number of unique items to score: {len(offers_to_score)}")

    ## Truncate test data if not all users are to be evaluated
    total_n_users = len(test_data["user_id"].unique())
    if not all_users:
        users_to_test = test_data["user_id"].unique()[
            : min(EVALUATION_USER_NUMBER, total_n_users)
        ]
        test_data = test_data[test_data.user_id.isin(users_to_test)]
        logger.info(f"Computing metrics for {len(users_to_test)} users")
    else:
        logger.info(f"Computing metrics for all users ({total_n_users}) users)")

    # TODO DELETE THIS AFTER BATCHING IS DONE
    offers_to_score = test_data.item_id.unique()
    logger.info(f"Number of unique items to score: {len(offers_to_score)}")

    # Create List to store predictions for each user
    list_df_predictions = []
    for current_user in tqdm(
        test_data["user_id"].unique(), mininterval=20, maxinterval=60
    ):
        # Prepare input for prediction
        prediction_input = [
            np.array([current_user] * len(offers_to_score)),
            offers_to_score,
        ]

        # Generate predictions
        prediction = model.predict(prediction_input, verbose=0)

        # Append predictions to final predictions DataFrame
        current_user_predictions = pd.DataFrame(
            {
                "user_id": [current_user] * len(offers_to_score),
                "item_id": offers_to_score.flatten().tolist(),
                "score": prediction.flatten().tolist(),
            }
        )
        list_df_predictions.append(current_user_predictions)

    df_predictions = pd.concat(list_df_predictions, ignore_index=True)
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

    # These metrics are not computed correctly for now, will be fixed once we filter the (user_id, item_id) pairs that are both in train and test sets.
    coverage = catalog_coverage(
        train_data, test_data, col_user=col_user, col_item=col_item
    )

    novelty_metric = novelty(
        train_data, test_data, col_user=col_user, col_item=col_item
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
