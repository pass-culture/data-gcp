from catboost import Pool
import numpy as np
from pcvalidation.utils.env_vars import UNUSED_COLS


def preprocess(data, params):
    """
    Preprocessing steps:
        - Fill integer null values with 0
        - Fill string null values with "none"
        - Convert boolean columns to int
    """
    try:
        del data["offer_id"]
    except KeyError:
        pass

    for key in data.keys():
        if key in params["text_features"]:
            data[key] = "" if data[key] is None else str(data[key])
        if key in params["numerical_features"]:
            data[key] = 0 if data[key] is None else int(data[key])
        # if key in params["boolean_features"]:
        #     if data[key] is None:
        #         data[key] = False
        #     else:
        #         data[key] = 1 if data[key] is True else 0
    return data


def convert_data_to_catboost_pool(data, features_type_dict):
    """
    Convert json data to catboost pool:
        - inputs:
            - Features names: List of features name (same order as list of features)
            - cat_features: list of categorical features names
            - text_features: list of text features names
            - embedding_features: list of embedding features names
        - output:
            - catboost pool
    """
    data_input = [list(data.values())]
    print("data_input: ", data_input)
    print("feature_names: ", list(data.keys()))
    pool = Pool(
        data=data_input,
        feature_names=list(data.keys()),
        cat_features=features_type_dict["cat_features"],
        text_features=features_type_dict["text_features"],
        embedding_features=features_type_dict["embedding_features"],
    )
    return pool
