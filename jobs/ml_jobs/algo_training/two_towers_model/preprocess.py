import json

import numpy as np
import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

from commons.constants import CONFIGS_PATH, MODEL_DIR, STORAGE_PATH
from commons.data_collect_queries import read_from_gcs


def get_features_by_type(feature_layers: dict, layer_types: list):
    return [
        col for col, layer in feature_layers.items() if layer["type"] in layer_types
    ]


def collect_prev_next(group):
    import numpy as np

    # Sort the group by 'event_date'
    sub_df = group[["item_id", "event_date"]]
    # Sort the group by 'event_date'
    sub_df = sub_df.sort_values("event_date").reset_index(drop=True)

    # Get list of booking_ids
    booking_ids = sub_df["item_id"].values

    # Create arrays to hold previous and next booking ids
    previous_booking_ids = np.empty(len(booking_ids), dtype=object)
    next_booking_ids = np.empty(len(booking_ids), dtype=object)

    # Fill previous_booking_ids
    for i in tqdm(
        range(len(booking_ids)),
        total=booking_ids.shape[0],
        desc="Processing previous ids",
        mininterval=30,
    ):
        previous_booking_ids[i] = booking_ids[max(i - 10, 0) : i].tolist()

    # Fill next_booking_ids
    next_booking_ids[:-1] = booking_ids[1:]
    next_booking_ids[-1] = None

    # Assign the previous and next booking ids to the group DataFrame
    group["previous_item_id"] = previous_booking_ids
    group["next_item_id"] = next_booking_ids
    return group


def preprocess(
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    input_dataframe_file_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    output_dataframe_file_name: str = typer.Option(
        ...,
        help="Name of the cleaned dataframe",
    ),
):
    """
    Preprocessing steps:
        - Fill integer null values with 0
        - Fill string null values with "none"
        - Convert numerical columns to int
    """
    raw_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_dataframe_file_name
    )
    logger.info(f"Data shape: {raw_data.shape}")
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)
    tqdm.pandas()
    integer_features = get_features_by_type(
        feature_layers=features["user_embedding_layers"], layer_types=["int"]
    ) + get_features_by_type(
        feature_layers=features["item_embedding_layers"], layer_types=["int"]
    )
    string_features = get_features_by_type(
        feature_layers=features["user_embedding_layers"],
        layer_types=["string", "text", "pretrained"],
    ) + get_features_by_type(
        feature_layers=features["item_embedding_layers"],
        layer_types=["string", "text", "pretrained"],
    )
    # This cover the case were 'user_id' is not a features of the model
    # Since we need user_id for evaluation purposes
    logger.info("Preprocessing features..")
    if "user_id" not in integer_features + string_features:
        string_features.append("user_id")
    clean_data = (
        raw_data[integer_features + string_features + ["event_date"]]
        .fillna({col: "none" for col in string_features})
        .fillna({col: 0 for col in integer_features})
        .astype({col: "int" for col in integer_features})
    )
    # clean_data["timestamp"] = clean_data["event_date"].apply(pd.Timestamp)
    clean_data["timestamp"] = pd.to_datetime(clean_data["event_date"])
    clean_data["timestamp"] = clean_data["timestamp"].astype(np.int64) // 10**9
    # booking_data=clean_data[clean_data['event_type'] == 'BOOKING']
    # Convert 'event_date' from Unix timestamp to datetime for sorting

    clean_data["event_date"] = pd.to_datetime(clean_data["event_date"])

    # # Step 1: Sort the DataFrame by 'user_id' and 'event_date'
    clean_data = clean_data.sort_values(by=["user_id", "event_date"]).reset_index(
        drop=True
    )
    logger.info(f"Data shape after preprocessing: {clean_data.shape}")
    logger.info("Collecting previous and next booking_ids..")
    clean_data = (
        clean_data.groupby("user_id").apply(collect_prev_next).reset_index(drop=True)
    )
    clean_data = clean_data.sample(frac=1).reset_index(drop=True)
    ## Add sequential bookings_ids
    clean_data.to_parquet(f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet")


if __name__ == "__main__":
    typer.run(preprocess)
