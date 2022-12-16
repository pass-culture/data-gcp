from typing import Tuple

import pandas as pd

from tools.split_data_tools import (
    split_by_column_and_ratio,
    split_by_ratio,
    reassign_extra_data_to_target,
)
from utils import STORAGE_PATH


def split_by_column_and_ratio(
    df: pd.DataFrame, column_name: str, ratio: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Sample the df by a ratio after grouping it by a column"""
    df_1 = df.groupby([column_name]).sample(frac=ratio)
    df_2 = df.loc[~df.index.isin(list(df_1.index))]
    return df_1, df_2


def split_by_ratio(df: pd.DataFrame, ratio: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Sample the df by a ratio"""
    df_1 = df.sample(frac=ratio)
    df_2 = df.loc[~df.index.isin(list(df_1.index))]
    return df_1, df_2


def reassign_extra_data_to_target(
    source_df: pd.DataFrame, target_df: pd.DataFrame, column_name: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Reassign the values of column_name present in source_df and not in the target_df into the target_df"""
    target_data = target_df[column_name].unique()
    target_df = pd.concat(
        [target_df, source_df[lambda df: ~df[column_name].isin(target_data)]]
    )
    if len(source_df[lambda df: df[column_name].isin(target_data)]) > 0:
        source_df = source_df[lambda df: df[column_name].isin(target_data)]
    return source_df, target_df


def main(storage_path: str):
    """We want to split the dataset into train, eval & test datasets but we want each dataset to contain
    at least one information about every user & offer the model was trained with.

    To do so:
        - We sample the clean_data by 80% after grouping it by users to create the train set
        - We split the remaining data in half to create test & eval sets
        - We reassign the offers present in eval & test and not in train into the train set
    """
    clean_data = pd.read_csv(f"{storage_path}/clean_data.csv")

    # Sample 80% of the clean data based on users
    positive_data_train, positive_data_not_in_train = split_by_column_and_ratio(
        df=clean_data, column_name="user_id", ratio=0.8
    )

    # Split the remaining data in half
    positive_data_eval, positive_data_test = split_by_ratio(
        df=positive_data_not_in_train, ratio=0.5
    )

    # Reassign the non evaluable data into train
    positive_data_eval, positive_data_train = reassign_extra_data_to_target(
        source_df=positive_data_eval,
        target_df=positive_data_train,
        column_name="item_id",
    )
    positive_data_test, positive_data_train = reassign_extra_data_to_target(
        source_df=positive_data_eval,
        target_df=positive_data_train,
        column_name="item_id",
    )

    # Store the datasets
    positive_data_train.to_csv(f"{storage_path}/positive_data_train.csv", index=False)
    positive_data_eval.to_csv(f"{storage_path}/positive_data_eval.csv", index=False)
    positive_data_test.to_csv(f"{storage_path}/positive_data_test.csv", index=False)


if __name__ == "__main__":
    main(STORAGE_PATH)
