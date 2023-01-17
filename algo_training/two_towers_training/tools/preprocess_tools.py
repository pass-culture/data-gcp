from typing import Any

import pandas as pd


def fill_na_by_feature_type(df: pd.DataFrame, columns_to_fill: list, fill_value: Any):
    return df.fillna({col: fill_value for col in columns_to_fill})


def get_features_by_type(feature_layers: dict, layer_types: list):
    return [
        col for col, layer in feature_layers.items() if layer["type"] in layer_types
    ]
