import json
import typer
import pandas as pd

from utils.constants import STORAGE_PATH, BASE_DIR
from two_towers_model.utils.constants import CONFIGS_PATH


def get_features_by_type(feature_layers: dict, layer_types: list):
    return [
        col for col, layer in feature_layers.items() if layer["type"] in layer_types
    ]


def preprocess(
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    dataframe_file_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
):
    """
    Preprocessing steps:
        - Fill integer null values with 0
        - Fill string null values with "none"
        - Convert numerical columns to int
    """
    raw_data = pd.read_csv(f"{STORAGE_PATH}/{dataframe_file_name}.csv")

    with open(
        f"{BASE_DIR}/{CONFIGS_PATH}/{config_file_name}.json", mode="r", encoding="utf-8"
    ) as config_file:
        features = json.load(config_file)

    integer_features = get_features_by_type(
        feature_layers=features["user_embedding_layers"], layer_types=["int"]
    ) + get_features_by_type(
        feature_layers=features["item_embedding_layers"], layer_types=["int"]
    )
    string_features = get_features_by_type(
        feature_layers=features["user_embedding_layers"], layer_types=["string", "text"]
    ) + get_features_by_type(
        feature_layers=features["item_embedding_layers"], layer_types=["string", "text"]
    )

    clean_data = (
        raw_data[integer_features + string_features]
        .fillna({col: "none" for col in string_features})
        .fillna({col: 0 for col in integer_features})
        .astype({col: "int" for col in integer_features})
    )

    clean_data.to_csv(f"{STORAGE_PATH}/{dataframe_file_name}.csv", index=False)


if __name__ == "__main__":
    typer.run(preprocess)
