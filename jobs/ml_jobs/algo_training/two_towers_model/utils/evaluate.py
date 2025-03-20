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
        train_dataset_name (str): Filename of train dataset in storage.
        test_dataset_name (str): Filename of test dataset in storage.
        list_k (List[int]): List of k values (top-k cutoff) for metrics evaluation.
        all_users (bool): Whether to evaluate all users or a subset.
        dummy (bool): Whether to also compute metrics for random and popularity baselines.
        quantile_threshold (float): Quantile threshold to filter top popular items in popularity baseline.

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


def plot_metrics_evolution(metrics, list_k, figures_folder):
    """
    Plot the evolution of metrics with different k values

    Args:
        metrics: Dictionary containing metrics for different k values
        figures_folder: Folder to save the plot

    Returns:
        None
    """
    logger.info("Creating metrics evolution plot")

    # Prepare data for plotting
    precision_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("precision")
    ]
    recall_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("recall")
    ]
    coverage_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("coverage")
    ]
    novelty_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("novelty")
    ]

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot each metric
    ax.plot(list_k, precision_values, marker="o", label="Precision")
    ax.plot(list_k, recall_values, marker="s", label="Recall")
    ax.plot(list_k, coverage_values, marker="^", label="Coverage")
    ax.plot(list_k, novelty_values, marker="d", label="Novelty")

    ax.set_xlabel("k (Number of recommendations)")
    ax.set_ylabel("Score")
    ax.set_title("Evolution of Metrics with k")
    ax.grid(True)
    ax.legend()

    # Save the plot
    plot_path = f"{figures_folder}/metrics_evolution.png"
    fig.savefig(plot_path)
    logger.info(f"Metrics evolution plot saved to {plot_path}")


## Baselines: random and most popular


def generate_random_baseline(
    test_data: pd.DataFrame,
    num_recommendations: int,
    specific_users: List[str],
) -> pd.DataFrame:
    """
    Generate random item recommendations for each user.

    Args:
        test_data (pd.DataFrame): DataFrame with at least a 'user_id' and 'item_id' column.
        num_recommendations (int): Number of random items to recommend per user.
        specific_users (List[str]): List of user IDs to generate recommendations for.

    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], where score is random.
    """
    # Unique items to randomly pick from
    unique_items = test_data["item_id"].unique()

    # Unique users
    df_users = pd.DataFrame({"user_id": specific_users}).drop_duplicates()

    # Repeat each user for num_recommendations and assign random items and scores
    df_random = (
        df_users.loc[df_users.index.repeat(num_recommendations)]
        .reset_index(drop=True)
        .assign(
            item_id=np.random.choice(
                unique_items, size=len(df_users) * num_recommendations, replace=True
            ),
            score=np.random.rand(len(df_users) * num_recommendations),
        )
    )
    return df_random


def generate_popularity_baseline(
    test_data: pd.DataFrame,
    num_recommendations: int,
    specific_users: List[str],
) -> pd.DataFrame:
    """
    Recommend the most popular items based on past interactions.

    Args:
        test_data (pd.DataFrame): DataFrame with at least 'user_id' column.
        num_recommendations (int): Number of items to recommend per user.
        specific_users (List[str]): List of user IDs to generate recommendations for.

    Returns:
        pd.DataFrame: Recommendations DataFrame with columns ['user_id', 'item_id', 'score'].
    """

    # Compute item popularity
    top_items = test_data["item_id"].value_counts().index.tolist()[:num_recommendations]

    # Prepare users to recommend to
    df_users = pd.DataFrame({"user_id": specific_users}).drop_duplicates()

    # Repeat users and assign items and scores
    df_popular = (
        df_users.loc[df_users.index.repeat(num_recommendations)]
        .reset_index(drop=True)
        .assign(
            item_id=np.random.choice(
                top_items, size=len(df_users) * num_recommendations, replace=True
            ),
            score=np.random.rand(len(df_users) * num_recommendations),
        )
    )
    return df_popular
