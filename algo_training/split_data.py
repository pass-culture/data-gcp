import pandas as pd
import pandas_gbq as gbq
from utils import STORAGE_PATH, MODEL_NAME, GCP_PROJECT_ID, ENV_SHORT_NAME


def split_data(storage_path: str, model_name: str):
    """We want to split the dataset into train, eval & test datasets but we want each dataset to contain
    at least one information about every user & offer the model was trained with.

    To do so:
        - We sample the clean_data by 80% after grouping it by users to create the train set
        - We split the remaining data in half to create test & eval sets
        - We reassign the offers present in eval & test and not in train into the train set
    """
    clean_data = pd.read_csv(f"{storage_path}/clean_data.csv")

    # Sample 80% of the clean data based on users
    positive_data_train = clean_data.groupby(["user_id"]).sample(frac=0.8)

    # Split the remaining data in half
    positive_data_not_in_train = clean_data.loc[
        lambda df: ~df.index.isin(list(positive_data_train.index))
    ]
    positive_data_eval = positive_data_not_in_train.sample(frac=0.5)
    positive_data_test = positive_data_not_in_train.loc[
        lambda df: ~df.index.isin(list(positive_data_eval.index))
    ]

    # Reassign the non evaluable data into train
    training_items = positive_data_train["item_id"].unique()

    positive_data_train = pd.concat(
        [
            positive_data_train,
            positive_data_eval[lambda df: ~df["item_id"].isin(training_items)],
            positive_data_test[lambda df: ~df["item_id"].isin(training_items)],
        ]
    )
    positive_data_eval = positive_data_eval[
        lambda df: df["item_id"].isin(training_items)
    ]
    positive_data_test = positive_data_test[
        lambda df: df["item_id"].isin(training_items)
    ]

    # Store the datasets
    positive_data_train.to_csv(f"{storage_path}/positive_data_train.csv", index=False)
    positive_data_eval.to_csv(f"{storage_path}/positive_data_eval.csv", index=False)
    positive_data_test.to_csv(f"{storage_path}/positive_data_test.csv", index=False)


if __name__ == "__main__":
    split_data(STORAGE_PATH, MODEL_NAME)
