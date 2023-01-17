import json
import typer
import pandas as pd

from tools.constants import CONFIG_FEATURES_PATH, STORAGE_PATH
from tools.preprocess_tools import fill_na_by_feature_type, get_features_by_type


def preprocess(
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
):
    """
    Preprocessing steps:
        - Fill integer null values with 0
        - Fill string null values with "none"
        - Convert numerical columns to int
    """
    raw_data = pd.read_csv(f"{STORAGE_PATH}/raw_data.csv")

    with open(
        CONFIG_FEATURES_PATH + f"/{config_file_name}.json", mode="r", encoding="utf-8"
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
        raw_data.pipe(
            fill_na_by_feature_type, columns_to_fill=integer_features, fill_value=0
        )
        .pipe(
            fill_na_by_feature_type, columns_to_fill=string_features, fill_value="none"
        )
        .astype({col: "int" for col in integer_features})
    )

    clean_data.to_csv(f"{STORAGE_PATH}/clean_data.csv", index=False)


if __name__ == "__main__":
    typer.run(preprocess)
