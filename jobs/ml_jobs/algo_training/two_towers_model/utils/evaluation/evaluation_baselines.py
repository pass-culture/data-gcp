from typing import List

import numpy as np
import pandas as pd

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
