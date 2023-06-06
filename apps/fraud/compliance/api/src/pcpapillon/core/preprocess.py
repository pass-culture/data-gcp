from catboost import Pool
import numpy as np
from pcpapillon.utils.env_vars import API_PARAMS
from pcpapillon.utils.data_model import Config
from pcpapillon.core.extract_embedding import extract_embedding

api_config = Config.from_dict(API_PARAMS)


def preprocess(data, prepoc_models):
    """
    Preprocessing steps:
        - prepare features
        - convert json data to catboost pool
    """
    data_clean = prepare_features(data, api_config.preprocess_features_type)
    data_w_emb = extract_embedding(
        data_clean, api_config.features_to_extract_embedding, prepoc_models
    )
    pool = convert_data_to_catboost_pool(data_w_emb, api_config.catboost_features_types)
    return pool, data_w_emb


def prepare_features(data, params):
    """
    Prepare features:
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
    pool = Pool(
        data=data_input,
        feature_names=list(data.keys()),
        cat_features=features_type_dict["cat_features"],
        text_features=features_type_dict["text_features"],
        embedding_features=features_type_dict["embedding_features"],
    )
    return pool
