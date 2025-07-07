from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from loguru import logger
from scipy.sparse import csr_matrix
from scipy.sparse.linalg import svds
from tqdm import tqdm

N_LATENT_VECTORS = 100


def build_interaction_matrix(df: pd.DataFrame) -> Dict:
    """
    Builds a sparse interaction matrix from user-item interactions in a DataFrame.
    This function converts user-item interaction data into a sparse CSR matrix
    representation where each row corresponds to a user and each column to an item.
    A value of 1 indicates that an interaction occurred between the user and item.
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing user-item interactions.
        Must have 'user_id' and 'item_id' columns.
    Returns
    -------
    Dict
        A dictionary containing:
        - 'csr_matrix': scipy.sparse.csr_matrix representing the user-item interactions
        - 'user_to_index': dict mapping user IDs to row indices
        - 'item_to_index': dict mapping item IDs to column indices
    Examples
    --------
    >>> interactions = pd.DataFrame({
    ...     'user_id': [1, 1, 2, 3],
    ...     'item_id': [101, 102, 101, 103]
    ... })
    >>> result = build_interaction_matrix(interactions)
    >>> result['csr_matrix'].shape
    (3, 3)
    """

    # Get unique users and items
    unique_users = df["user_id"].unique()
    unique_items = df["item_id"].unique()

    # Create mappings from IDs to indices
    user_to_index = {user: i for i, user in enumerate(unique_users)}
    item_to_index = {item: i for i, item in enumerate(unique_items)}

    # Create sparse matrix coordinates
    user_indices = [user_to_index[user] for user in df["user_id"]]
    item_indices = [item_to_index[item] for item in df["item_id"]]
    values = [1] * len(df)  # Assuming binary interactions (1 = interaction occurred)

    # Create the sparse matrix
    return {
        "csr_matrix": csr_matrix(
            (values, (user_indices, item_indices)),
            shape=(len(unique_users), len(unique_items)),
        ),
        "user_to_index": user_to_index,
        "item_to_index": item_to_index,
    }


def compute_svd(
    interaction_matrix: csr_matrix, k: int
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute truncated SVD on the interaction matrix

    Parameters:
        interaction_matrix: The sparse user-item interaction matrix
        k: Number of singular values/vectors to compute

    Returns:
        U: User latent factors
        sigma: Singular values
        Vt: Item latent factors
    """
    # Ensure k is not larger than min dimension
    k = min(k, min(interaction_matrix.shape) - 1)

    # Compute the truncated SVD
    U, sigma, Vt = svds(interaction_matrix, k=k)

    # Sort the results by singular values in descending order
    idx = np.argsort(-sigma)
    sigma = sigma[idx]
    U = U[:, idx]
    Vt = Vt[idx, :]

    return U, sigma, Vt


def filter_predictions(
    df_predictions: pd.DataFrame,
    train_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Filter out items that users have already interacted with in training data.
    Args:
        df_predictions (pd.DataFrame): DataFrame containing predictions with columns 'user_id' and 'item_id'.
        train_data (pd.DataFrame): DataFrame containing training data with columns 'user_id' and 'item_id'.

    Returns:
        pd.DataFrame: DataFrame containing filtered predictions .
    """
    # merge df_predictions with train_data on user_id and item_id while keeping indicator
    df_predictions_filtered = df_predictions.merge(
        train_data, on=["user_id", "item_id"], how="left", indicator=True
    )
    # drop rows where indicator is not left_only (meaning the (user, item) was also in training data)
    df_predictions_filtered = df_predictions_filtered[
        df_predictions_filtered["_merge"] == "left_only"
    ].drop(columns=["_merge"])

    return df_predictions_filtered


def generate_random_baseline(
    test_data: pd.DataFrame,
    train_data: pd.DataFrame,
    num_recommendations: int,
    specific_users: List[str],
) -> pd.DataFrame:
    """
    Generate random item recommendations for each user.

    Args:
        test_data (pd.DataFrame): DataFrame with at least a 'user_id' and 'item_id' column.
        train_data (pd.DataFrame): DataFrame with at least a 'user_id' and 'item_id' column.
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

    # return filtered out items that users have already interacted with in training data by performing an anti-join
    return filter_predictions(df_random, train_data)


def generate_popularity_baseline(
    test_data: pd.DataFrame,
    train_data: pd.DataFrame,
    num_recommendations: int,
    specific_users: List[str],
) -> pd.DataFrame:
    """
    Recommend the most popular items based on past interactions.

    Args:
        test_data (pd.DataFrame): DataFrame with at least 'user_id' column.
        train_data (pd.DataFrame): DataFrame with at least 'user_id' and 'item_id' columns.
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

    # return filtered out items that users have already interacted with in training data by performing an anti-join
    return filter_predictions(df_popular, train_data)


def generate_svd_baseline(
    test_data: pd.DataFrame,
    train_data: pd.DataFrame,
    num_recommendations: int,
    specific_users: List[str],
) -> pd.DataFrame:
    """
    Recommend items based on SVD decomposition of user-item interactions.

    Args:
        test_data (pd.DataFrame): DataFrame with at least 'user_id' column.
        train_data (pd.DataFrame): DataFrame with at least 'user_id' and 'item_id' columns.
        num_recommendations (int): Number of items to recommend per user.
        specific_users (List[str]): List of user IDs to generate recommendations for.

    Returns:
        pd.DataFrame: Recommendations DataFrame with columns ['user_id', 'item_id', 'score'].
    """

    # Build the user-item interaction matrix
    svd_data = build_interaction_matrix(train_data)
    interaction_matrix = svd_data["csr_matrix"]
    user_to_index = svd_data["user_to_index"]
    item_to_index = svd_data["item_to_index"]

    # Compute the SVD
    U, sigma, Vt = compute_svd(interaction_matrix, k=N_LATENT_VECTORS)

    # Create mappings
    offers_to_score = test_data.item_id.unique()
    mapped_items_to_score = [item_to_index.get(item) for item in offers_to_score]
    mapped_users_to_score = [
        user_to_index.get(user) for user in specific_users if user in user_to_index
    ]
    reverse_user_mapping = {v: k for k, v in user_to_index.items()}

    # Filter out None values from mapped_items
    valid_indices = []
    valid_mapped_items = []
    for i, mapped_item in enumerate(mapped_items_to_score):
        if mapped_item is not None:
            valid_indices.append(i)
            valid_mapped_items.append(mapped_item)

    # Get corresponding valid items
    valid_items = offers_to_score[valid_indices]

    # Extract item factors for all valid items
    item_factors = Vt[:, valid_mapped_items]

    list_df_predictions = []
    for mapped_current_user in tqdm(
        mapped_users_to_score, mininterval=20, maxinterval=60
    ):
        # Skip if user not in training data
        if mapped_current_user is None:
            continue

        try:
            # Get user latent factors and scale by singular values
            user_factors_scaled = U[mapped_current_user, :] * sigma

            # Compute scores for all valid items at once using matrix multiplication
            scores = np.dot(user_factors_scaled, item_factors)

            # Create user predictions dataframe
            if len(valid_items) > 0:
                user_predictions = pd.DataFrame(
                    {
                        "user_id": [reverse_user_mapping[mapped_current_user]]
                        * len(valid_items),
                        "item_id": valid_items,
                        "score": scores,
                    }
                )
                # Select only top num_recommendations items for this user
                user_predictions = user_predictions.sort_values(
                    by="score", ascending=False
                ).head(num_recommendations)

                list_df_predictions.append(user_predictions)

        except Exception as e:
            logger.warning(
                f"Error generating predictions for user {mapped_current_user}: {e}"
            )
            continue

    # Combine all user predictions
    if list_df_predictions:
        df_predictions = pd.concat(list_df_predictions, ignore_index=True)
    else:
        df_predictions = pd.DataFrame(columns=["user_id", "item_id", "score"])

    # Filter out items that users have already interacted with
    return filter_predictions(df_predictions, train_data)
