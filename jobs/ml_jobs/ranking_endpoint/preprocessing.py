import pandas as pd

from app.model import DEFAULT_NUMERICAL

FEATURES_CONSTRUCTION = {
    "user_deposit_remaining_credit": lambda df: df["user_deposit_amount"]
    - df["user_amount_spent"],
    "offer_semantic_emb_mean": DEFAULT_NUMERICAL,
    "offer_item_score": DEFAULT_NUMERICAL,
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


def map_features_columns(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.assign(**FEATURES_CONSTRUCTION)
        .fillna(DEFAULT_NUMERICAL)
        .rename(columns=FEATURES_MAPPING)
    )
