from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs
from loguru import logger
from recommenders.evaluation.python_evaluation import (
    catalog_coverage,
    novelty,
    precision_at_k,
    recall_at_k,
)
from tqdm import tqdm

from commons.constants import (
    ALL_USERS,
    ENV_SHORT_NAME,
    EVALUATION_USER_NUMBER,
    LIST_K,
    USER_BATCH_SIZE,
)
from commons.data_collect_queries import read_from_gcs
from two_towers_model.utils.baseline_prediction import (
    filter_predictions,
    generate_popularity_baseline,
    generate_random_baseline,
    generate_svd_baseline,
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


def get_scann_params(env_short_name: str, max_k: int) -> Dict[str, Any]:
    if env_short_name == "prod":
        logger.info("Using ScaNN params for prod")
        return {
            "k": max_k,
            "distance_measure": "dot_product",
            "num_leaves": 500,
            "num_leaves_to_search": 50,
            "training_iterations": 20,
            "parallelize_batch_searches": True,
            "num_reordering_candidates": max_k,
        }

    else:
        logger.info("Using ScaNN params for ehp")
        return {
            "distance_measure": "dot_product",
            "num_leaves": 1,
            "num_leaves_to_search": 1,
        }


def get_offers_to_score(test_data: pd.DataFrame) -> np.ndarray:
    """
    Get unique items to score and duplicate them if necessary to meet ScaNN requirements.

    Args:
        test_data (pd.DataFrame): DataFrame containing test data with 'item_id' column.

    Returns:
        numpy array of unique items to score.
    """
    offers_to_score = test_data.item_id.unique()
    if len(offers_to_score) < 20:
        # ScaNN requires at least 16 items to score, so duplicate items if necessary
        offers_to_score = np.repeat(offers_to_score, 50 // len(offers_to_score))
    logger.info(f"Number of unique items to score: {len(offers_to_score)}")
    return offers_to_score


def get_users_to_test(test_data: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
    """
    Get users to test and filter test data based on all_users flag.

    Args:
        test_data (pd.DataFrame): DataFrame containing test data with 'user_id' column.

    Returns:
        Tuple[pd.DataFrame, numpy array]: Tuple containing the filtered test data and the list of users to test.
    """
    users_to_test = test_data["user_id"].unique()
    total_n_users = len(users_to_test)
    if not ALL_USERS:
        users_to_test = users_to_test[: min(EVALUATION_USER_NUMBER, total_n_users)]
        test_data = test_data[test_data.user_id.isin(users_to_test)]
        logger.info(f"Computing metrics for {len(users_to_test)} users")
    else:
        logger.info(f"Computing metrics for all users ({total_n_users}) users)")
    return test_data, users_to_test


def generate_predictions(
    model,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    max_k: int,
) -> pd.DataFrame:
    """
    Generates item recommendations for each user in the test dataset using ScaNN indexing.
    The recommendations are computed using the user embeddings from the model's user tower.

    Args:
        model: Trained Two tower retrieval model.
        train_data (pd.DataFrame): DataFrame containing train user-item interactions with at least
            'user_id' and 'item_id' columns.
        test_data (pd.DataFrame): DataFrame containing test user-item interactions with at least
            'user_id' and 'item_id' columns.
        max_k (int): Maximum number of recommendations to generate.
               This number will be augmented by 100 recommendations to account for the items that users have already interacted with in training data.
    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], where score represents
                        the similarity score between user and item embeddings.
    """
    # hedge against training filter
    max_k = max_k + 100

    # Get unique items to score
    offers_to_score = get_offers_to_score(test_data)

    # Get item embeddings for the corpus
    offers_to_score_embeddings = tf.cast(model.item_layer(offers_to_score), tf.float32)

    # Truncate test data if not all users are to be evaluated
    test_data, users_to_test = get_users_to_test(test_data)

    # Initialize ScaNN index with appropriate parameters tuned for env.
    scann = tfrs.layers.factorized_top_k.ScaNN(
        **get_scann_params(ENV_SHORT_NAME, max_k)
    )
    scann_index = scann.index(
        candidates=offers_to_score_embeddings,
        identifiers=tf.constant(offers_to_score),
    )

    list_predictions_dict = []
    logger.info(f"Using batch size of {USER_BATCH_SIZE} users")
    for batch_start_index in tqdm(
        range(0, len(users_to_test), USER_BATCH_SIZE), mininterval=20, maxinterval=60
    ):
        # Get recommendations using ScaNN
        batch_users = users_to_test[
            batch_start_index : batch_start_index + USER_BATCH_SIZE
        ]
        user_embeddings = model.user_layer(batch_users)
        scores, candidates = scann_index(user_embeddings)

        list_predictions_dict.append(
            {
                "user_id": np.concatenate(
                    [
                        [user] * len(candidate)
                        for user, candidate in zip(batch_users, candidates)
                    ]
                ),
                "item_id": candidates.numpy().flatten(),
                "score": scores.numpy().flatten(),
            }
        )

    # Combine all batches
    df_predictions = pd.concat(
        [
            pd.DataFrame(batch_predictions)
            for batch_predictions in list_predictions_dict
        ],
        ignore_index=True,
    ).astype({"item_id": str})  # convert bytes to string

    # Return filtered out items that users have already interacted with in training data by performing an anti-join
    return filter_predictions(df_predictions, train_data)


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

    # Filter df_predictions to keep top k recommendations because original catalog_coverage and novelty  do not support filtering by k
    df_predictions_diversity_metrics = (
        df_predictions.groupby("user_id")
        .apply(lambda x: x.head(k))
        .reset_index(drop=True)
    )
    coverage = catalog_coverage(
        train_data,
        df_predictions_diversity_metrics,
        col_user=col_user,
        col_item=col_item,
    )
    novelty_metric = novelty(
        train_data,
        df_predictions_diversity_metrics,
        col_user=col_user,
        col_item=col_item,
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

    logger.info("Inferring model predictions")
    df_predictions = generate_predictions(
        model=model,
        train_data=data_dict["train"],
        test_data=data_dict["test"],
        max_k=max(LIST_K),
    )
    list_predictions_to_evaluate = [{"predictions": df_predictions, "prefix": ""}]

    if dummy:
        # Extract unique users from model predictions to ensure consistent user set
        predicted_users = df_predictions["user_id"].unique().tolist()
        logger.info(f"Using {len(predicted_users)} users for baseline evaluations")

        logger.info("Generating SVD baseline predictions")
        list_predictions_to_evaluate.append(
            {
                "predictions": generate_svd_baseline(
                    test_data=data_dict["test"],
                    train_data=data_dict["train"],
                    num_recommendations=max(LIST_K),
                    specific_users=predicted_users,
                ),
                "prefix": "svd_",
            }
        )

        logger.info("Generating random baseline predictions")
        list_predictions_to_evaluate.append(
            {
                "predictions": generate_random_baseline(
                    test_data=data_dict["test"],
                    train_data=data_dict["train"],
                    num_recommendations=max(LIST_K),
                    specific_users=predicted_users,
                ),
                "prefix": "random_",
            }
        )

        logger.info("Generating popularity baseline predictions")
        list_predictions_to_evaluate.append(
            {
                "predictions": generate_popularity_baseline(
                    test_data=data_dict["test"],
                    train_data=data_dict["train"],
                    num_recommendations=max(LIST_K),
                    specific_users=predicted_users,
                ),
                "prefix": "popular_",
            }
        )

    logger.info("Computing metrics")
    metrics = {}
    for predictions in list_predictions_to_evaluate:
        for k in LIST_K:
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
