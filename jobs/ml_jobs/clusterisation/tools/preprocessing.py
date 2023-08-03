import pandas as pd
import umap
from tools.utils import categories_config, convert_str_emb_to_float
from loguru import logger


def prepare_pretrained_encoding(data):
    pretrained_embedding_float = convert_str_emb_to_float(data)
    pretrained_embedding_df = pd.DataFrame(
        pretrained_embedding_float,
        columns=[f"t{index}" for index in range(len(pretrained_embedding_float[0]))],
    )
    logger.info(
        f"pretrained_embedding_df columns: {pretrained_embedding_df.columns.tolist()}"
    )
    return pretrained_embedding_df


def prepare_categorical_fields(items):
    items = items.replace("None", "")
    items = items.fillna("")

    item_by_category = items[
        [
            "item_id",
            "category",
            "subcategory_id",
            "offer_sub_type_label",
            "offer_type_label",
        ]
    ]
    item_by_category = item_by_category.melt(id_vars=["item_id"])
    item_by_category["categ"] = (
        item_by_category["variable"] + " " + item_by_category["value"]
    )
    item_by_category = item_by_category.drop(columns=["variable", "value"])

    item_category_encoded = pd.pivot_table(
        item_by_category,
        index=["item_id"],
        columns=["categ"],
        values=["item_id"],
        aggfunc="size",
    ).reset_index()

    item_category_encoded = item_category_encoded.fillna(0)
    item_category_encoded = item_category_encoded.loc[
        ~(item_category_encoded["offer_type_label "] > 1)
    ]
    item_category_encoded = pd.merge(
        item_category_encoded,
        item_by_category[["item_id"]],
        how="inner",
        on=["item_id"],
    )

    item_category_encoded_matrix = item_category_encoded.drop(
        columns=["item_id"]
        # columns=["index", "item_id", "product_id_size"]
    )
    results = item_category_encoded_matrix.values.tolist()
    reduction = 2
    red = umap.UMAP(
        n_components=reduction,
        random_state=42,
        n_neighbors=10,
        transform_seed=42,
        verbose=True,
    )
    red.fit(results)
    X = red.transform(results)

    category_encoding_reduced = pd.DataFrame(X)
    category_encoding_reduced.columns = [
        str(column) for column in category_encoding_reduced.columns
    ]
    item_by_category_encoded_reduced = pd.concat(
        [
            category_encoding_reduced.reset_index(drop=True),
            item_category_encoded[["item_id"]].reset_index(drop=True),
        ],
        axis=1,
    )
    # categ_red = categ_red.rename(columns={'0': 'c0', '1': 'c1', '2': 'c2', '3': 'c3', '4': 'c4'})
    item_by_category_encoded_reduced = item_by_category_encoded_reduced.rename(
        columns={"0": "c0", "1": "c1"}
    )
    # categ_red
    return item_by_category_encoded_reduced, item_by_category


def prepare_clusterisation(
    item_semantic_encoding, item_by_category_encoded_reduced, item_by_category
):

    item_full_encoding = pd.merge(
        item_by_category_encoded_reduced,
        item_semantic_encoding,
        how="right",
        on=["item_id"],
    )

    item_full_encoding[["c0", "c1"]] = item_full_encoding[["c0", "c1"]].fillna(
        item_full_encoding[["c0", "c1"]].mean()
    )

    offers_categ_group = pd.DataFrame()
    item_by_macro_cat = (
        item_by_category.groupby(["categ"])
        .agg(item_id_size=("item_id", "size"))
        .reset_index()
    )
    item_by_macro_cat = item_by_macro_cat.sort_values(by=["categ"], ascending=[True])
    data = item_by_category
    for category in categories_config:
        feature_list = "|".join(
            [
                f"""{feature["type"]} {feature["name"]}"""
                for feature in category["features"]
            ]
        )
        df_tmp = data.loc[
            data["categ"].str.contains(feature_list, case=False, regex=True, na=False)
        ]
        df_tmp = df_tmp.drop_duplicates(subset=["item_id"], keep="first")
        df_tmp["category_group"] = category["group"]
        df_tmp = df_tmp[["item_id", "category_group"]]
        df_tmp = df_tmp.drop_duplicates(keep="first")
        offers_categ_group = pd.concat(
            [df_tmp, offers_categ_group], axis=0, ignore_index=True
        )

    item_full_encoding_w_cat_group = pd.merge(
        item_full_encoding, offers_categ_group, how="inner", on=["item_id"]
    )

    return item_full_encoding_w_cat_group
