import json

import numpy as np
import pandas as pd

from app.model import (
    CATEGORICAL_FEATURES,
    DEFAULT_CATEGORICAL,
    DEFAULT_NUMERICAL,
    EMBEDDING_DIM,
    NUMERIC_FEATURES,
    ClassMapping,
)

FEATURES_CONSTRUCTION = {
    "day_of_the_week": lambda df: pd.Categorical(
        df["day_of_week"],
        categories=[
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ],
        ordered=True,
    ).codes,
    "offer_stock_beginning_days": lambda df: -df["offer_stock_beginning_days"],
    "offer_creation_days": lambda df: -df["offer_creation_days"],
    "user_x_date_id": lambda df: df["user_id"].astype(str)
    + "_"
    + df["event_date"].astype(str),
}

FEATURES_MAPPING = {
    "user_centroid_x": "user_iris_x",
    "user_centroid_y": "user_iris_y",
    "item_booking_number_last_7_days": "offer_booking_number_last_7_days",
    "item_booking_number_last_14_days": "offer_booking_number_last_14_days",
    "item_booking_number_last_28_days": "offer_booking_number_last_28_days",
    "offer_mean_stock_price": "offer_stock_price",
    "offer_created_delta_in_days": "offer_creation_days",
    "offer_max_stock_beginning_days": "offer_stock_beginning_days",
    "hour_of_day": "hour_of_the_day",
    "module_type": "context",
    "item_user_similarity": "offer_item_score",
    "semantic_emb_mean": "offer_semantic_emb_mean",
    "user_theoretical_remaining_credit": "user_deposit_remaining_credit",
}


def map_features_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply feature construction and column mapping to a DataFrame.
    This function performs two main operations on the input DataFrame:
    1. Constructs new features using the FEATURES_CONSTRUCTION mapping
    2. Renames columns according to the FEATURES_MAPPING dictionary
    Args:
        df (pd.DataFrame): Input DataFrame to transform
    Returns:
        pd.DataFrame: Transformed DataFrame with new features constructed and columns renamed
    Note:
        Requires FEATURES_CONSTRUCTION and FEATURES_MAPPING to be defined in the module scope.
    """

    return df.assign(**FEATURES_CONSTRUCTION).rename(columns=FEATURES_MAPPING)


def preprocess_embeddings(
    data: pd.DataFrame, embedding_dim: int, default_numerical: float
) -> pd.DataFrame:
    """
    Preprocess embedding data by converting JSON string embeddings to individual feature columns.
    This function handles two scenarios:
    1. If embedding_dim is 0, drops embedding columns entirely
    2. If embedding_dim > 0, converts JSON string embeddings into separate numerical columns
    Args:
        data (pd.DataFrame): Input DataFrame containing 'user_embedding_json' and 'item_embedding_json' columns
        embedding_dim (int): Dimension of embeddings. If 0, embedding columns are dropped.
        default_numerical (float): Default value to use for missing or null embeddings
    Returns:
        pd.DataFrame: Processed DataFrame with either:
            - Embedding columns removed (if embedding_dim=0), or
            - JSON embeddings converted to individual columns named 'user_emb_{i}' and 'item_emb_{i}'
    Notes:
        - Missing embeddings (NaN or "null") are filled with arrays of default_numerical values
        - Original embedding JSON columns are removed from the output
        - Output maintains the same row indices as input DataFrame
    """

    if embedding_dim == 0:
        # If no embeddings are used, we can drop the embedding columns
        return data.drop(columns=["user_embedding_json", "item_embedding_json"])

    # If embeddings are used, we need to process them
    missing_array = json.dumps(np.array([default_numerical] * embedding_dim).tolist())

    # Stack arrays into 2D NumPy arrays
    data_with_string_embeddings_df = data.assign(
        user_embedding=lambda df: df.user_embedding_json.fillna(missing_array)
        .replace("null", missing_array)
        .apply(lambda x: np.array(json.loads(x))),
        item_embedding=lambda df: df.item_embedding_json.fillna(missing_array)
        .replace("null", missing_array)
        .apply(lambda x: np.array(json.loads(x))),
    )
    user_embeddings_array = np.stack(
        data_with_string_embeddings_df["user_embedding"].values
    )
    item_embeddings_array = np.stack(
        data_with_string_embeddings_df["item_embedding"].values
    )

    # Convert to DataFrames with proper indices and column names
    user_embedding_features = pd.DataFrame(
        user_embeddings_array,
        index=data_with_string_embeddings_df.index,
        columns=[f"user_emb_{i}" for i in range(embedding_dim)],
    )
    item_embedding_features = pd.DataFrame(
        item_embeddings_array,
        index=data_with_string_embeddings_df.index,
        columns=[f"item_emb_{i}" for i in range(embedding_dim)],
    )

    return pd.concat(
        [
            data_with_string_embeddings_df.drop(
                columns=[
                    "user_embedding_json",
                    "item_embedding_json",
                    "user_embedding",
                    "item_embedding",
                ]
            ),
            user_embedding_features,
            item_embedding_features,
        ],
        axis=1,
    )


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    typed_data_df = (
        data.loc[
            :,
            lambda df: df.columns.isin(
                [
                    "is_seen",
                    "is_consulted",
                    "is_booked",
                    "user_id",
                    "item_id",
                    "unique_session_id",
                    "user_x_date_id",
                ]
                + NUMERIC_FEATURES
                + CATEGORICAL_FEATURES
                + ["user_embedding_json", "item_embedding_json"]
            ),
        ]
        .fillna({numeric_feat: DEFAULT_NUMERICAL for numeric_feat in NUMERIC_FEATURES})
        .fillna({cat_feat: DEFAULT_CATEGORICAL for cat_feat in CATEGORICAL_FEATURES})
        .astype({numeric_feat: "float" for numeric_feat in NUMERIC_FEATURES})
        .astype({cat_feat: "str" for cat_feat in CATEGORICAL_FEATURES})
    )

    data_with_status_df = (
        typed_data_df.fillna({"is_consulted": 0.0, "is_booked": 0.0})
        .astype(
            {
                "is_consulted": "float",
                "is_booked": "float",
            }
        )
        .rename(
            columns={
                "is_consulted": ClassMapping.consulted.name,
                "is_booked": ClassMapping.booked.name,
            }
        )
        .assign(
            status=lambda df: pd.Series(
                index=df.index, data=[ClassMapping.seen.name] * len(df)
            )
            .where(
                df[ClassMapping.consulted.name] != 1.0,
                other=ClassMapping.consulted.name,
            )
            .where(
                df[ClassMapping.booked.name] != 1.0,
                other=ClassMapping.booked.name,
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
        .drop_duplicates()
    )

    return preprocess_embeddings(
        data=data_with_status_df,
        embedding_dim=EMBEDDING_DIM,
        default_numerical=DEFAULT_NUMERICAL,
    )
