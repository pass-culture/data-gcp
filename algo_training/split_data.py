import os
import pandas as pd


STORAGE_PATH = os.environ.get("STORAGE_PATH", "")


def split_data(storage_path: str):
    bookings = pd.read_csv(f"{storage_path}/clean_data.csv")

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


if __name__ == "__main__":
    split_data(STORAGE_PATH)
