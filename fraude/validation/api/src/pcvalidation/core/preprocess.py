from catboost import Pool

from pcvalidation.utils.env_vars import UNUSED_COLS


def preprocess(data, params):
    try:
        del data["offer_id"]
    except KeyError:
        pass

    for key in data.keys():
        if key in params["text_features"]:
            if data[key] is None:
                data[key] = ""
            else:
                data[key] = str(data[key])
        if key in params["numerical_features"]:
            if data[key] is None:
                data[key] = 0
            else:
                data[key] = int(data[key])
        if key in params["boolean_features"]:
            if data[key] is None:
                data[key] = False
            else:
                data[key] = 1 if data[key] == True else 0
    return data


def convert_dataframe_to_catboost_pool(data, features_type_dict):
    data_input = [list(data.values())]
    # print(f"keys:{list(data.keys())}")
    # print(f"values:{list(data.values())}")
    pool = Pool(
        data_input,
        feature_names=list(data.keys()),
        cat_features=features_type_dict["cat_features"],
        text_features=features_type_dict["text_features"],
        embedding_features=features_type_dict["embedding_features"],
    )
    return pool
