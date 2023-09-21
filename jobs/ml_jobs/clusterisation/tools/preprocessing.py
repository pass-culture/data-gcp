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


def prepare_categorical_fields(items, splitting):
    items = items.replace("None", "")
    items = items.fillna("")
    useless_df = []
    item_clean = items[
        [
            "item_id",
            "rank",
            "category",
            "subcategory_id",
            "offer_sub_type_label",
            "offer_type_label",
        ]
    ]
    useless_df.append(items)
    logger.info(f"item_clean: {len(item_clean)}")
    (
        item_by_macro_cat,
        item_by_category_fitted,
        item_by_category_predicted,
    ) = split_data(splitting, item_clean)

    logger.info(f"item_by_category_fitted: {len(item_by_category_fitted)}")
    logger.info(f"item_by_category_predicted: {len(item_by_category_predicted)}")
    (
        fitted_data_encoded_matrix,
        predicted_data_encoded_matrix,
        item_category_encoded_item_ids
        ) = encode_items(
        item_by_category_fitted[["item_id","categ"]], item_by_category_predicted[["item_id","categ"]]
    )
    # useless_df.append(predicted_data_encoded)
    # useless_df.append(fitted_data_encoded)
    del useless_df
    # spliting data

    item_by_category_encoded_reduced = reduce_item_encoding(
        fitted_data_encoded_matrix,
        predicted_data_encoded_matrix,
        item_category_encoded_item_ids
    )
    
    

    return item_by_category_encoded_reduced, item_by_macro_cat


def reduce_item_encoding(
    fitted_data_encoded_matrix,
    predicted_data_encoded_matrix,
    item_category_encoded_item_ids
):
    data_to_fit = fitted_data_encoded_matrix.values.tolist()
    data_to_predict = predicted_data_encoded_matrix.values.tolist()
    reduction = 2
    red = umap.UMAP(
        n_components=reduction,
        random_state=42,
        n_neighbors=10,
        transform_seed=42,
        verbose=True,
    )
    logger.info(f"Fitting Umap reduction, with {len(data_to_fit)} items...")
    red.fit(data_to_fit)
    logger.info(f"Transform fitted data...")
    X = red.transform(data_to_fit)
    logger.info(f"Transform {len(data_to_predict)} items to predict...")
    
    X_predicted = red.transform(data_to_predict)

    category_encoding_reduced_fitted = pd.DataFrame(X)
    category_encoding_reduced_predicted = pd.DataFrame(X_predicted)
    category_encoding_reduced = pd.concat(
        [category_encoding_reduced_fitted, category_encoding_reduced_predicted],
        ignore_index=True,
    )

    # useless_df = [category_encoding_reduced_fitted, category_encoding_reduced_predicted]
    # del useless_df

    category_encoding_reduced.columns = [
        str(column) for column in category_encoding_reduced.columns
    ]
    item_by_category_encoded_reduced = pd.concat(
        [
            category_encoding_reduced.reset_index(drop=True),
            item_category_encoded_item_ids,
        ],
        axis=1,
    )
    # useless_df = [category_encoding_reduced, item_category_encoded_item_ids]
    # del useless_df
    item_by_category_encoded_reduced = item_by_category_encoded_reduced.rename(
        columns={"0": "c0", "1": "c1"}
    )

    return item_by_category_encoded_reduced


def encode_items(item_by_category_fitted, item_by_category_predicted):
    
    fitted_data_encoded_matrix = pd.pivot_table(
        item_by_category_fitted,
        index=["item_id"],
        columns=["categ"],
        values=["item_id"],
        aggfunc="size",
    ).reset_index()

    fitted_data_encoded_matrix = fitted_data_encoded_matrix.fillna(0)

    fitted_data_encoded_matrix = pd.merge(
        fitted_data_encoded_matrix,
        item_by_category_fitted[["item_id"]],
        how="inner",
        on=["item_id"],
    )
    # ---
    predicted_data_encoded_matrix = pd.pivot_table(
        item_by_category_predicted,
        index=["item_id"],
        columns=["categ"],
        values=["item_id"],
        aggfunc="size",
    ).reset_index()

    predicted_data_encoded_matrix = predicted_data_encoded_matrix.fillna(0)

    predicted_data_encoded_matrix = pd.merge(
        predicted_data_encoded_matrix,
        item_by_category_predicted[["item_id"]],
        how="inner",
        on=["item_id"],
    )
    predicted_data_encoded_item_ids = predicted_data_encoded_matrix[["item_id"]].reset_index(
        drop=True
    )
    fitted_data_encoded_item_ids = fitted_data_encoded_matrix[["item_id"]].reset_index(
        drop=True
    )

    item_category_encoded_item_ids = pd.concat(
        [fitted_data_encoded_item_ids, predicted_data_encoded_item_ids],
        ignore_index=True,
    )
    fitted_data_encoded_matrix = fitted_data_encoded_matrix.drop(columns=["item_id"])
    
    predicted_data_encoded_matrix = predicted_data_encoded_matrix.drop(columns=["item_id"])
    

    return (
        fitted_data_encoded_matrix,
        predicted_data_encoded_matrix,
        item_category_encoded_item_ids
        )


def split_data(splitting, item_clean):
    
    item_by_category = get_item_by_categories(item_clean)
    item_by_category = pd.merge(
        item_by_category,
        item_clean[["item_id"]],
        how="inner",
        on=["item_id"],
    )
    item_by_macro_cat = (
        item_by_category.groupby(["categ"])
        .agg(item_id_size=("item_id", "size"))
        .reset_index()
    )
    
    splitt_row_number = int((len(item_clean) + 1) * (int(splitting) / 100))
    fitted_data = item_clean.iloc[:splitt_row_number]
    predicted_data = item_clean.iloc[splitt_row_number:]
    logger.info(f" fitted_data: {len(fitted_data)} items...")
    logger.info(f" predicted_data: {len(data_to_fit)} items...")
    item_by_category_fitted = get_item_by_categories(fitted_data)
    item_by_category_predicted = get_item_by_categories(predicted_data)
    logger.info(f" item_by_category_fitted: {len(item_by_category_fitted)} items...")
    logger.info(f" item_by_category_predicted: {len(item_by_category_predicted)} items...")
    return (
        item_by_macro_cat,
        item_by_category_fitted,
        item_by_category_predicted,
    )


def get_item_by_categories(data):
    data = data.drop(["rank"], axis=1).melt(id_vars=["item_id"])
    data["categ"] = (
        data["variable"] + " " + data["value"]
    )
    data = data.drop(columns=["variable", "value"])
    data=data.drop_duplicates(
        subset=["item_id"], keep="first"
    )
    return data


def prepare_clusterisation(item_full_encoding, item_by_macro_cat):

    # logger.info(f"""item_semantic_encoding: {len(item_semantic_encoding)}""")
    # logger.info(
    #     f"""item_by_category_encoded_reduced: {len(item_by_category_encoded_reduced)}"""
    # )
    # item_semantic_encoding = item_semantic_encoding.drop_duplicates(
    #     subset=["item_id"], keep="first"
    # )
    # item_by_category_encoded_reduced = item_by_category_encoded_reduced.drop_duplicates(
    #     subset=["item_id"], keep="first"
    # )
    # item_full_encoding = pd.merge(
    #     item_semantic_encoding,
    #     item_by_category_encoded_reduced,
    #     how="left",
    #     on=["item_id"],
    #     validate="one_to_one",
    # )
    # logger.info(f"""item_full_encoding 0.0: {len(item_full_encoding)}""")
    # item_full_encoding[["c0", "c1"]] = item_full_encoding[["c0", "c1"]].fillna(
    #     item_full_encoding[["c0", "c1"]].mean()
    # )
    logger.info(f"""item_full_encoding 1.0: {len(item_full_encoding)}""")
    offers_categ_group = pd.DataFrame()
    # item_by_macro_cat = (
    #     item_by_category.groupby(["categ"])
    #     .agg(item_id_size=("item_id", "size"))
    #     .reset_index()
    # )
    item_by_macro_cat = item_by_macro_cat.sort_values(by=["categ"], ascending=[True])
    offers_categ_group = get_item_from_config_cats(item_by_macro_cat)
    logger.info(f"""offers_categ_group: {len(offers_categ_group)}""")
    logger.info(f"""item_full_encoding 2.0: {len(item_full_encoding)}""")
    item_full_encoding_w_cat_group = pd.merge(
        item_full_encoding, offers_categ_group, how="inner", on=["item_id"]
    )
    logger.info(
        f"""item_full_encoding_w_cat_group: {len(item_full_encoding_w_cat_group)}"""
    )
    return item_full_encoding_w_cat_group


def get_item_from_config_cats(item_by_macro_cat):
    data = item_by_macro_cat
    # data = item_by_category
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

    return offers_categ_group
