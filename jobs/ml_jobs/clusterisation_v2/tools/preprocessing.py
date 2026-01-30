from rich.pretty import d
import numpy as np
import pandas as pd


def prepare_embedding(data: np.ndarray, pretrained_embedding_size: int) -> pd.DataFrame:
    """
    Prepare the embedding data for clustering.

    Args:
        data (list): The list of embeddings.
        pretrained_embedding_size (int): The size of the pretrained embedding.

    Returns:
        pd.DataFrame : A DataFrame containing the embeddings."""
    embedding_df = pd.DataFrame(
        data=data,
        columns=[f"t{index}" for index in range(pretrained_embedding_size)],
    )
    return embedding_df


def get_item_by_categories(data: pd.DataFrame) -> pd.DataFrame:
    """Get the item by categories."""
    data = data.melt(id_vars=["item_id"])
    data["categ"] = data["variable"] + " " + data["value"]
    data = data.drop(columns=["variable", "value"])
    return data


def get_item_by_group(data: pd.DataFrame, group_config: dict) -> pd.DataFrame:
    """
    Retrieves items from the given DataFrame based on the specified group configuration.

    Args:
        data (pd.DataFrame): The DataFrame containing the items.
        group_config (dict): The configuration specifying the groups and their features.

    Returns:
        pd.DataFrame: A DataFrame containing the items grouped by category.

    """
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
