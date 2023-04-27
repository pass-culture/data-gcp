from catboost import Pool
import pandas as pd

UNUSED_COLS = ["outing", "physical_goods"]


def preprocess(df):
    df = df.drop(columns=["offer_id"])
    columns = df.columns.tolist()
    for col in columns:
        if df[col].dtype == int or df[col].dtype == float:
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype(int)
        elif df[col].dtype.name == "boolean":
            df[col] = np.where(df[col] == True, 1, 0)
        else:
            df[col] = df[col].fillna("")
            df[col] = df[col].astype(str)
    return df


def convert_dataframe_to_catboost_pool(df, features_type_dict):
    pool = Pool(
        df,
        cat_features=features_type_dict["cat_features"],
        text_features=features_type_dict["text_features"],
        embedding_features=features_type_dict["embedding_features"],
    )
    return pool
