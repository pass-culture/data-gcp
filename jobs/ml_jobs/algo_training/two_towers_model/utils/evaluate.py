import secrets
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
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
from sklearn.decomposition import PCA
from tqdm import tqdm

from commons.constants import EVALUATION_USER_NUMBER
from commons.data_collect_queries import read_from_gcs


def load_data_for_evaluation(
    storage_path: str,
    training_dataset_name: str,
    test_dataset_name: str,
) -> Dict[str, pd.DataFrame]:
    """
    Loads training and test datasets for evaluation by reading data from storage,
    keeping only necessary columns and removing duplicates.

    Args:
        storage_path (str): Path to the storage location (e.g., GCS bucket path).
        training_dataset_name (str): Name of the training dataset file.
        test_dataset_name (str): Name of the test dataset file.

    Returns:
        Dict[str, pd.DataFrame]: Dictionary with keys 'train' and 'test' containing the datasets.
    """

    list_columns_to_keep = ["user_id", "item_id"]

    logger.info("Load training")
    training_data = read_from_gcs(storage_path, training_dataset_name, parallel=False)[
        list_columns_to_keep
    ].drop_duplicates()
    logger.info(
        f"Number of unique (user, item) interactions in training data: {training_data.shape[0]}"
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

    return {"train": training_data, "test": test_data}


def generate_predictions(
    model, test_data: pd.DataFrame, all_users: bool
) -> pd.DataFrame:
    """
    Generates item dot product scores for each user in the test dataset using the provided model.
    The scores are computed for each user with all unique (item_id) in test_dataset which are stored in offers_to_score.
    offers_to_score does not contain previously seen items during training which prevents data leak.

    Args:
        model: Trained Two tower retrieval model.
        test_data (pd.DataFrame): DataFrame containing test user-item interactions with at least
            'user_id' and 'item_id' columns.
        all_users : bool: If True, evaluate all users in the test dataset. If False, evaluate only a subset of users.

    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], where score represents
                        the predicted interaction score (dot product between user and item two tower embeddings).
    """

    # Get unique items to score
    offers_to_score = test_data.item_id.unique()

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

    # Create List to store predictions for each user
    list_df_predictions = []
    for current_user in tqdm(test_data["user_id"].unique()):
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
    training_data: pd.DataFrame,
    k: int,
    prefix: Optional[str] = "",
) -> Dict[str, float]:
    """
    Computes evaluation metrics at top-k: precision, recall, coverage, and novelty.

    Args:
        test_data (pd.DataFrame): Ground truth user-item interactions (columns: 'user_id', 'item_id').
        df_predictions (pd.DataFrame): Predicted scores for user-item pairs
                                       (columns: 'user_id', 'item_id', 'score').
        training_data (pd.DataFrame): Historical training interactions (columns: 'user_id', 'item_id').
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

    # TODO change coveage and novelty calls after filtering items in training data, also slice reco_df to top k
    # Note that coverage and novelty are computed for each k:
    # coverage@k will mean if we only retrieved k items, how many of the catalogue would be covered. Same for novelty.
    # is this a good approach for our usecase?
    coverage = catalog_coverage(
        training_data, test_data, col_user=col_user, col_item=col_item
    )

    novelty_metric = novelty(
        training_data, test_data, col_user=col_user, col_item=col_item
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
    training_dataset_name: str,
    test_dataset_name: str,
    list_k: List[int],
    all_users: bool,
    dummy: bool,
    quantile_threshold: float,
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
        training_dataset_name (str): Filename of training dataset in storage.
        test_dataset_name (str): Filename of test dataset in storage.
        list_k (List[int]): List of k values (top-k cutoff) for metrics evaluation.
        all_users (bool): Whether to evaluate all users or a subset.
        dummy (bool): Whether to also compute metrics for random and popularity baselines.
        quantile_threshold (float): Quantile threshold to filter top popular items in popularity baseline.

    Returns:
        Dict[str, float]: Dictionary containing evaluation metrics for each k value.
    """

    logger.info("Get data for evaluation")
    data_dict = load_data_for_evaluation(
        storage_path=storage_path,
        training_dataset_name=training_dataset_name,
        test_dataset_name=test_dataset_name,
    )

    logger.info("Getting predictions")
    df_predictions = generate_predictions(
        model=model, test_data=data_dict["test"], all_users=all_users
    )

    logger.info("Computing model metrics")
    metrics = {}
    for k in list_k:
        metrics.update(
            compute_metrics(
                test_data=data_dict["test"],
                df_predictions=df_predictions,
                training_data=data_dict["train"],
                k=k,
                prefix="",
            )
        )

    if dummy:
        logger.info("Generating random and popularity baselines")
        df_random = generate_random_baseline(
            test_data=data_dict["test"],
            all_users=all_users,
            num_recommendations=max(list_k),
        )
        df_popular = generate_popularity_baseline(
            training_data=data_dict["train"],
            test_data=data_dict["test"],
            all_users=all_users,
            num_recommendations=max(list_k),
            quantile_threshold=quantile_threshold,
        )

        logger.info("Computing metrics for random and popularity baselines")
        for k in list_k:
            metrics.update(
                compute_metrics(
                    test_data=data_dict["test"],
                    df_predictions=df_random,
                    training_data=data_dict["train"],
                    k=k,
                    prefix="random_",
                )
            )
            metrics.update(
                compute_metrics(
                    test_data=data_dict["test"],
                    df_predictions=df_popular,
                    training_data=data_dict["train"],
                    k=k,
                    prefix="popular_",
                )
            )

    return metrics


def save_pca_representation(
    loaded_model: tf.keras.models.Model,
    item_data: pd.DataFrame,
    figures_folder: str,
) -> None:
    """
    Computes a 2D PCA projection of item embeddings from the retrieval model
    and generates scatter plots for visualizing item distributions by category and subcategory.

    Args:
        loaded_model (tf.keras.models.Model): Trained retrieval model from which item embeddings are extracted.
        item_data (pd.DataFrame): DataFrame containing metadata for items, including 'item_id',
            'offer_category_id', and 'offer_subcategory_id'.
        figures_folder (str): Path to the folder where the generated plots will be saved.

    Returns:
        None. Saves visualizations as PDF files in the specified folder.
    """
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    seed = secrets.randbelow(1000)
    logger.info(f"Random state for PCA fixed to seed={seed}")
    pca_out = PCA(n_components=2, random_state=seed).fit_transform(embeddings)
    categories = item_data["offer_category_id"].unique().tolist()
    item_representation = pd.DataFrame(
        {
            "item_id": item_ids,
            "x": pca_out[:, 0],
            "y": pca_out[:, 1],
        }
    ).merge(item_data, on=["item_id"], how="inner")

    colormap = plt.cm.tab20.colors
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    for idx, category in enumerate(categories):
        data = item_representation.loc[lambda df: df["offer_category_id"] == category]
        max_plots = min(data.shape[0], 10000)
        data = data.sample(n=max_plots)
        ax.scatter(
            data["x"].values,
            data["y"].values,
            s=10,
            color=colormap[idx],
            label=category,
            alpha=0.7,
        )
        logger.info(f"Plotting {len(data)} points for category {category}")
        fig_sub, ax_sub = plt.subplots(1, 1, figsize=(15, 10))
        for idx_sub, subcategory in enumerate(data["offer_subcategory_id"].unique()):
            data_sub = data.loc[lambda df: df["offer_subcategory_id"] == subcategory]
            ax_sub.scatter(
                data_sub["x"].values,
                data_sub["y"].values,
                s=10,
                color=colormap[idx_sub],
                label=subcategory,
                alpha=0.7,
            )
        ax_sub.legend()
        ax_sub.grid(True)
        fig_sub.savefig(figures_folder + f"{category}.pdf")

    ax.legend()
    ax.grid(True)
    fig.savefig(figures_folder + "ALL_CATEGORIES.pdf")


## Baselines: random and most popular


def generate_random_baseline(
    test_data: pd.DataFrame,
    all_users: bool,
    num_recommendations: int,
) -> pd.DataFrame:
    """
    Generate random item recommendations for each user.

    Args:
        test_data (pd.DataFrame): DataFrame with at least a 'user_id' and 'item_id' column.
        all_users (bool): If True, generate recommendations for all users in test_data.
                          If False, limit to a sample of users (EVALUATION_USER_NUMBER).
        num_recommendations (int): Number of random items to recommend per user.

    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], where score is random.
    """
    # Unique items to randomly pick from
    unique_items = test_data["item_id"].unique()

    # Unique users
    df_users = test_data[["user_id"]].drop_duplicates().copy()

    # Optionally sample users
    if not all_users:
        users_to_test = df_users["user_id"].unique()[
            : min(EVALUATION_USER_NUMBER, len(df_users))
        ]
        df_users = df_users[df_users["user_id"].isin(users_to_test)]

    # Repeat each user for num_recommendations
    df_random = df_users.loc[df_users.index.repeat(num_recommendations)].reset_index(
        drop=True
    )

    # Assign random items and scores
    df_random["item_id"] = np.random.choice(
        unique_items, size=len(df_random), replace=True
    )
    df_random["score"] = np.random.rand(len(df_random))

    return df_random


def generate_popularity_baseline(
    training_data: pd.DataFrame,
    test_data: pd.DataFrame,
    all_users: bool,
    num_recommendations: int,
    quantile_threshold: float,
) -> pd.DataFrame:
    """
    Recommend the most popular items based on past interactions.

    Args:
        training_data (pd.DataFrame): Historical user-item interaction data with at least 'item_id'.
        test_data (pd.DataFrame): DataFrame with at least 'user_id' column.
        all_users (bool): If True, recommend for all users. If False, limit to a sample of users.
        num_recommendations (int): Number of items to recommend per user.
        quantile_threshold (float): Threshold to consider top X% most popular items (0-1 range).

    Returns:
        pd.DataFrame: Recommendations DataFrame with columns ['user_id', 'item_id', 'score'].
    """

    # Compute item popularity
    item_popularity = training_data["item_id"].value_counts().reset_index()
    item_popularity.columns = ["item_id", "popularity"]

    # Select top items based on quantile threshold
    popularity_cutoff = item_popularity["popularity"].quantile(quantile_threshold)
    top_items = item_popularity[item_popularity["popularity"] >= popularity_cutoff][
        "item_id"
    ].tolist()

    # If not enough items, take top N
    if len(top_items) < num_recommendations:
        top_items = item_popularity["item_id"].tolist()[:num_recommendations]

    # Prepare users to recommend to
    df_users = test_data[["user_id"]].drop_duplicates().copy()

    if not all_users:
        users_to_test = df_users["user_id"].unique()[
            : min(EVALUATION_USER_NUMBER, len(df_users))
        ]
        df_users = df_users[df_users["user_id"].isin(users_to_test)]

    # Expand users to assign num_recommendations per user
    df_popular = df_users.loc[df_users.index.repeat(num_recommendations)].reset_index(
        drop=True
    )

    # Assign items (cycled if fewer top items than recommendations needed)
    repeated_items = (top_items * (len(df_popular) // len(top_items) + 1))[
        : len(df_popular)
    ]
    df_popular["item_id"] = repeated_items

    # Assign random score per recommendation
    df_popular["score"] = np.random.rand(len(df_popular))

    return df_popular
