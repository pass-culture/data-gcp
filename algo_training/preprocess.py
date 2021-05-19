import os
import pandas as pd


def preprocess(storage_path: str):
    bookings = pd.read_csv(f"{storage_path}/raw_data.csv")
    bookings.rename(
        columns={"offer_id": "item_id", "nb_bookings": "rating"}, inplace=True
    )
    bookings.to_csv(f"{storage_path}/clean_data.csv")


def main():
    STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
    preprocess(STORAGE_PATH)


if __name__ == "__main__":
    main()
