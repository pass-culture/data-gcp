import os
import json
import gcsfs
import pandas as pd


def feature_engineering(storage_path: str):
    bookings = pd.read_csv(f"{storage_path}/clean_data.csv")
    bookings.rename(
        columns={"offer_id": "item_id", "nb_bookings": "rating"}, inplace=True
    )

    user_ids = bookings["user_id"].unique().tolist()
    user2user_encoded = {x: i for i, x in enumerate(user_ids)}
    userencoded2user = {i: x for i, x in enumerate(user_ids)}

    save_dict_to_path(user2user_encoded, f"{storage_path}/user2user_encoded.json")
    save_dict_to_path(userencoded2user, f"{storage_path}/userencoded2user.json")

    item_ids = bookings["item_id"].unique().tolist()
    item2item_encoded = {x: i for i, x in enumerate(item_ids)}
    item_encoded2item = {i: x for i, x in enumerate(item_ids)}

    save_dict_to_path(item2item_encoded, f"{storage_path}/item2item_encoded.json")
    save_dict_to_path(item_encoded2item, f"{storage_path}/item_encoded2item.json")

    bookings_from_id = bookings.copy()

    bookings["user_id"] = bookings["user_id"].map(user2user_encoded)
    bookings["item_id"] = bookings["item_id"].map(item2item_encoded)

    df = bookings.sample(frac=1).reset_index(drop=True)
    lim = df.shape[0] * 80 / 100
    pos_data_train = df[df.index < lim]
    pos_data_test = df[df.index >= lim]

    pos_data_train.to_csv(f"{storage_path}/pos_data_train.csv", index=False)
    pos_data_test.to_csv(f"{storage_path}/pos_data_test.csv", index=False)


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
