import json

import numpy as np
import pandas as pd

from app.model import (
    CATEGORICAL_FEATURES,
    DEFAULT_NUMERICAL,
    EMBEDDING_DIM,
    NUMERIC_FEATURES,
    ClassMapping,
)

FEATURES_CONSTRUCTION = {
    "user_deposit_remaining_credit": lambda df: df["user_deposit_amount"]
    - df["user_amount_spent"],
    "offer_semantic_emb_mean": DEFAULT_NUMERICAL,
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
}

FEATURES_MAPPING = {
    "interaction_is_geolocated": "user_is_geolocated",
    "user_centroid_x": "user_iris_x",
    "user_centroid_y": "user_iris_y",
    "item_booking_number_last_7_days": "offer_booking_number_last_7_days",
    "item_booking_number_last_14_days": "offer_booking_number_last_14_days",
    "item_booking_number_last_28_days": "offer_booking_number_last_28_days",
    "displayed_position": "offer_item_rank",
    "offer_mean_stock_price": "offer_stock_price",
    "offer_created_delta_in_days": "offer_creation_days",
    "offer_max_stock_beginning_days": "offer_stock_beginning_days",
    "hour_of_day": "hour_of_the_day",
    "module_type": "context",
    "item_user_similarity": "offer_item_score",
}


def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    # Add features horizontally
    user_embed_dim = EMBEDDING_DIM
    item_embed_dim = EMBEDDING_DIM
    missing_array = json.dumps(np.array([DEFAULT_NUMERICAL] * user_embed_dim).tolist())

    df = (
        data.loc[
            :,
            lambda df: df.columns.isin(
                ["is_seen", "is_consulted", "is_booked", "unique_session_id"]
                + NUMERIC_FEATURES
                + CATEGORICAL_FEATURES
                + ["user_embedding_json", "item_embedding_json"]
            ),
        ]
        .astype(
            {
                "is_consulted": "float",
                "is_booked": "float",
            }
        )
        .fillna({"is_consulted": 0, "is_booked": 0})
        .rename(
            columns={
                "is_consulted": ClassMapping.consulted.name,
                "is_booked": ClassMapping.booked.name,
            }
        )
        .assign(
            status=lambda df: pd.Series([ClassMapping.seen.name] * len(df))
            .where(
                df[ClassMapping.consulted.name] != 1.0,
                other=ClassMapping.consulted.name,
            )
            .where(df[ClassMapping.booked.name] != 1.0, other=ClassMapping.booked.name),
            target_class=lambda df: df["status"]
            .map(
                {
                    class_mapping.name: class_mapping.value
                    for class_mapping in ClassMapping
                }
            )
            .astype(int),
        )
    ).drop_duplicates()

    # Stack arrays into 2D NumPy arrays
    df = df.assign(
        user_embedding=lambda df: df.user_embedding_json.fillna(missing_array)
        .replace("null", missing_array)
        .apply(lambda x: np.array(json.loads(x))),
        item_embedding=lambda df: df.item_embedding_json.fillna(missing_array)
        .replace("null", missing_array)
        .apply(lambda x: np.array(json.loads(x))),
    )
    user_embeddings_array = np.stack(df["user_embedding"].values)
    item_embeddings_array = np.stack(df["item_embedding"].values)

    # Convert to DataFrames with proper indices and column names
    user_embedding_features = pd.DataFrame(
        user_embeddings_array,
        index=df.index,
        columns=[f"user_emb_{i}" for i in range(user_embed_dim)],
    )
    item_embedding_features = pd.DataFrame(
        item_embeddings_array,
        index=df.index,
        columns=[f"item_emb_{i}" for i in range(item_embed_dim)],
    )

    return pd.concat(
        [
            df.drop(
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


def map_features_columns(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(**FEATURES_CONSTRUCTION)
        .fillna(DEFAULT_NUMERICAL)
        .rename(columns=FEATURES_MAPPING)
    )
