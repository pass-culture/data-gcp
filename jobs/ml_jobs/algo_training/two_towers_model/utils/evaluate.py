import secrets
from typing import Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tensorflow as tf
from loguru import logger
from recommenders.evaluation.python_evaluation import (
    catalog_coverage,
    map_at_k,
    novelty,
    precision_at_k,
    r_precision_at_k,
    recall_at_k,
)
from sklearn.decomposition import PCA
from tqdm import tqdm

from commons.constants import (
    EVALUATION_USER_NUMBER,
)
from commons.data_collect_queries import read_from_gcs


def prepare_data_for_evaluation(
    storage_path: str,
    training_dataset_name: str,
    test_dataset_name: str,
    n_users_to_test: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.Series]]:
    """
    Prepares training and test datasets for evaluation by loading the data from storage,
    filtering necessary columns, removing duplicates, and optionally truncating the test users.

    Args:
        storage_path (str): Path to the storage location (GCS bucket path).
        training_dataset_name (str): Name of the training dataset file.
        test_dataset_name (str): Name of the test dataset file.
        n_users_to_test (Optional[int], optional): Number of users to include for evaluation.
            If provided, limits evaluation to the first `n_users_to_test` in the test dataset.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.Series]]:
            - Filtered test dataset as a DataFrame.
            - Filtered training dataset as a DataFrame.
            - Series of user IDs to evaluate (or None if all users are included).
    """

    list_columns_to_keep = ["user_id", "item_id", "offer_subcategory_id"]

    logger.info("Load training")
    training_data = read_from_gcs(storage_path, training_dataset_name, parallel=False)[
        list_columns_to_keep
    ].drop_duplicates()
    logger.info(
        f"Number of unique (user, item, offer_subcategory_id) interactions in training data: {training_data.shape[0]}"
    )

    logger.info("Load test data")
    test_data = (
        read_from_gcs(storage_path, test_dataset_name, parallel=False)
        .astype({"user_id": str, "item_id": str, "offer_subcategory_id": str})[
            list_columns_to_keep
        ]
        .drop_duplicates()
    )
    logger.info(
        f"Number of unique (user, item, offer_subcategory_id) interactions in test data: {test_data.shape[0]}"
    )

    ## Truncate test data if n_users is provided
    total_n_users = len(test_data["user_id"].unique())
    if n_users_to_test:
        users_to_test = test_data["user_id"].unique()[
            : min(n_users_to_test, total_n_users)
        ]
        logger.info(f"Computing metrics for {len(users_to_test)} users")
    else:
        users_to_test = None
        logger.info(f"Computing metrics for all users ({total_n_users}) users)")

    return test_data, training_data, users_to_test


def generate_predictions(
    model, test_data: pd.DataFrame, users_to_test: Optional[pd.Series]
) -> pd.DataFrame:
    """
    Generates item dot product scores for each user in the test dataset using the provided model.
    The scores are computed for each user with all unique (item_id, offer_subcategory_id) in test_dataset which are stored in offers_to_score.
    offers_to_score does not contain previously seen items during training which prevents data leak.

    Args:
        model: Trained Two tower retrieval model.
        test_data (pd.DataFrame): DataFrame containing test user-item interactions with at least
            'user_id', 'item_id', and 'offer_subcategory_id' columns.
        users_to_test (Optional[pd.Series]): Series of user IDs to generate predictions for.
            If None, predictions are generated for all users in the test_data.

    Returns:
        pd.DataFrame: DataFrame with columns ['user_id', 'item_id', 'score'], where score represents
                        the predicted interaction score (dot product between user and item two tower embeddings).
    """

    # Get unique items to score
    offers_to_score = (
        test_data[["item_id", "offer_subcategory_id"]]
        .drop_duplicates()
        .item_id.to_numpy()
    )

    # Create empty DataFrame to store predictions
    df_predictions = pd.DataFrame(columns=["user_id", "item_id", "score"])

    if users_to_test is not None:
        test_data = test_data[test_data.user_id.isin(users_to_test)]

    for current_user in tqdm(test_data["user_id"].unique()):
        input_feature = current_user

        # Prepare input for prediction
        prediction_input = [
            np.array([input_feature] * len(offers_to_score)),
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
        df_predictions = pd.concat([df_predictions, current_user_predictions])

    return df_predictions


def compute_metrics(
    test_data: pd.DataFrame,
    df_predictions: pd.DataFrame,
    training_data: pd.DataFrame,
    k: int,
) -> dict:
    """
    Evaluates the quality of retrievals using several standard metrics.

    Args:
        test_data (pd.DataFrame): DataFrame containing ground truth user-item interactions
            with columns 'user_id' and 'item_id'.
        df_predictions (pd.DataFrame): DataFrame containing predicted scores for user-item pairs
            with columns 'user_id', 'item_id', and 'score'.
        training_data (pd.DataFrame): DataFrame containing historical training data with
            'user_id' and 'item_id' columns, used for coverage and novelty computations.
        k (int): Number of top retrievals to consider for evaluation (e.g., precision@k, recall@k).

    Returns:
        dict: Dictionary containing evaluation metrics including:
            - precision@k
            - recall@k
            - RPrecision@k
            - map@k
            - novelty
            - coverage
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

    RPrecision = r_precision_at_k(
        test_data,
        df_predictions,
        col_user=col_user,
        col_item=col_item,
        col_prediction=col_prediction,
        relevancy_method="top_k",
        k=k,
    )

    map = map_at_k(
        test_data,
        df_predictions,
        col_user=col_user,
        col_item=col_item,
        col_prediction=col_prediction,
        relevancy_method="top_k",
        k=k,
    )

    coverage = catalog_coverage(
        training_data, test_data, col_user=col_user, col_item=col_item
    )

    novelty_metric = novelty(
        training_data, test_data, col_user=col_user, col_item=col_item
    )

    ## Assemble metrics in dict
    metrics = {
        f"precision@{k}": precision,
        f"recall@{k}": recall,
        f"RPrecision@{k}": RPrecision,
        f"map@{k}": map,
        f"novelty@{k}": novelty_metric,
        f"coverage@{k}": coverage,
    }
    return metrics


def evaluate(
    model: tf.keras.models.Model,
    storage_path: str,
    training_dataset_name: str = "recommendation_training_data",
    test_dataset_name: str = "recommendation_test_data",
    list_k: list = [10, 40],
) -> dict:
    """
    Runs the full evaluation pipeline for a retrieval model, including data preparation,
    prediction generation, and computation of evaluation metrics for different values of k.

    Args:
        model (tf.keras.models.Model): Trained retrieval model used to generate predictions.
        storage_path (str): Path to the storage location containing the datasets.
        training_dataset_name (str, optional): Name of the training dataset file. Defaults to "recommendation_training_data".
        test_dataset_name (str, optional): Name of the test dataset file. Defaults to "recommendation_test_data".
        list_k (list, optional): List of cutoff values (k) for which evaluation metrics should be computed. Defaults to [10, 40].

    Returns:
        dict: Dictionary containing evaluation metrics for each value of k in list_k.
    """

    logger.info("Get data for evaluation")
    test_data, training_data, users_to_test = prepare_data_for_evaluation(
        storage_path=storage_path,
        training_dataset_name=training_dataset_name,
        test_dataset_name=test_dataset_name,
        n_users_to_test=EVALUATION_USER_NUMBER,
    )

    logger.info("Get predictions")
    df_predictions = generate_predictions(model, test_data, users_to_test)

    logger.info("Compute metrics")
    metrics = {}
    for k in list_k:
        metrics.update(compute_metrics(test_data, df_predictions, training_data, k=k))

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
