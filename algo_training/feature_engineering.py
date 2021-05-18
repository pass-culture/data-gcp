import os
import json
import gcsfs
import pandas as pd


def feature_engineering(storage_path: str):
    bookings = pd.read_csv(f"{storage_path}/clean_data.csv")

    n_users = len(set(bookings.user_id.values))
    n_items = len(set(bookings.item_id.values))
    print(f"{n_users} users and {n_items} items")
    user_ids = bookings["user_id"].unique().tolist()
    item_ids = bookings["item_id"].unique().tolist()

    df = bookings.sample(frac=1).reset_index(drop=True)
    lim_train = df.shape[0] * 80 / 100
    lim_eval = df.shape[0] * 90 / 100
    pos_data_train = df.loc[df.index < lim_train]
    pos_data_eval = df.loc[df.index < lim_eval]
    pos_data_eval = pos_data_eval.loc[pos_data_eval.index >= lim_train]
    pos_data_test = df[df.index >= lim_eval]

    pos_data_train.to_csv(f"{storage_path}/pos_data_train.csv", index=False)
    pos_data_test.to_csv(f"{storage_path}/pos_data_test.csv", index=False)
    pos_data_eval.to_csv(f"{storage_path}/pos_data_eval.csv", index=False)


def save_dict_to_path(dictionnary, path):
    with fs.open(path, "w") as fp:
        json.dump(dictionnary, fp)


def main():
    feature_engineering(STORAGE_PATH)


if __name__ == "__main__":
    STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
    main()
