import pandas as pd

from utils import STORAGE_PATH, MODEL_NAME


def preprocess(storage_path: str):
    bookings = pd.read_csv(f"{storage_path}/raw_data.csv")
    bookings.rename(
        columns={"offer_id": "item_id", "nb_bookings": "rating"}, inplace=True
    )
    bookings.to_csv(f"{storage_path}/clean_data.csv")


def lighten_matrice(storage_path: str):
    clicks_df = pd.read_csv(f"{storage_path}/raw_data.csv")
    top_offers = clicks_df.item_id.value_counts()
    top_offers = top_offers[top_offers > 40].index.tolist()
    clicks_df_light = clicks_df[clicks_df.item_id.isin(top_offers)]

    top_users = clicks_df.user_id.value_counts()
    top_users = top_users[top_users > 2].index.tolist()
    clicks_df_light = clicks_df_light[clicks_df_light.user_id.isin(top_users)]
    clicks_df_light.to_csv(f"{storage_path}/clean_data.csv")
    return


def main():
    if MODEL_NAME == "v1":
        preprocess(STORAGE_PATH)
    if MODEL_NAME == "v2_deep_reco":
        lighten_matrice(STORAGE_PATH)


if __name__ == "__main__":
    main()
