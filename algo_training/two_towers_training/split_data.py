import pandas as pd
from tools.split_data_tools import (
    split_by_column_and_ratio,
    split_by_ratio,
    reassign_extra_data_to_target,
)
from loguru import logger

from utils.utils import STORAGE_PATH


def main(storage_path: str):
    """We want to split the dataset into train, eval & test datasets but we want each dataset to contain
    at least one information about every user & offer the model was trained with.

    To do so:
        - We sample the clean_data by 80% after grouping it by users to create the train set
        - We split the remaining data in half to create test & eval sets
        - We reassign the offers present in eval & test and not in train into the train set
    """
    clean_data = pd.read_csv(f"{storage_path}/clean_data.csv")

    logger.info(f"Original dataset size: {len(clean_data)}")

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

    logger.info(f"Train dataset size: {len(positive_data_train)}")
    logger.info(f"Evaluation dataset size: {len(positive_data_eval)}")
    logger.info(f"Test dataset size: {len(positive_data_test)}")

    # Store the datasets
    positive_data_train.to_csv(f"{storage_path}/positive_data_train.csv", index=False)
    positive_data_eval.to_csv(f"{storage_path}/positive_data_eval.csv", index=False)
    positive_data_test.to_csv(f"{storage_path}/positive_data_test.csv", index=False)


if __name__ == "__main__":
    main(STORAGE_PATH)
