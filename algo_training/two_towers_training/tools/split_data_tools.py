import pandas as pd


def split_by_column_and_ratio(
    df: pd.DataFrame, column_name: str, ratio: float, seed: int = None
):
    df_1 = df.groupby([column_name]).sample(frac=ratio, random_state=seed)
    df_2 = df.loc[~df.index.isin(list(df_1.index))]
    return df_1, df_2


def split_by_ratio(df: pd.DataFrame, ratio: float, seed: int = None):
    df_1 = df.sample(frac=ratio, random_state=seed)
    df_2 = df.loc[~df.index.isin(list(df_1.index))]
    return df_1, df_2


def reassign_extra_data_to_target(
    source_df: pd.DataFrame, target_df: pd.DataFrame, column_name: str
):
    target_data = target_df[column_name].unique()
    target_df = pd.concat(
        [target_df, source_df[lambda df: ~df[column_name].isin(target_data)]]
    )
    if len(source_df[lambda df: df[column_name].isin(target_data)]) > 0:
        source_df = source_df[lambda df: df[column_name].isin(target_data)]
    return source_df, target_df
