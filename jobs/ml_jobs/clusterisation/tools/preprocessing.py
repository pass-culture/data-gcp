import pandas as pd
from tools.utils import convert_str_emb_to_float
from loguru import logger


def prepare_embedding(data):
    embedding_float = convert_str_emb_to_float(data)
    embedding_df = pd.DataFrame(
        embedding_float,
        columns=[f"t{index}" for index in range(len(embedding_float[0]))],
    )
    return embedding_df


def get_item_by_categories(data):
    data = data.melt(id_vars=["item_id"])
    data["categ"] = data["variable"] + " " + data["value"]
    data = data.drop(columns=["variable", "value"])
    return data


def get_item_by_group(data, group_config):
    item_by_group = pd.DataFrame()
    for group in group_config:
        feature_list = "|".join(
            [
                f"""{feature["type"]} {feature["name"]}"""
                for feature in group["features"]
            ]
        )
        df_tmp = data.loc[
            data["categ"].str.contains(feature_list, case=False, regex=True, na=False)
        ]

        df_tmp = df_tmp.drop_duplicates(subset=["item_id"], keep="first")
        df_tmp["category_group"] = group["group"]
        df_tmp = df_tmp[["item_id", "categ", "category_group"]]
        df_tmp = df_tmp.drop_duplicates(keep="first")
        item_by_group = pd.concat([df_tmp, item_by_group], axis=0, ignore_index=True)

    return item_by_group
