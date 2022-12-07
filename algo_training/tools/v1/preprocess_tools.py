import pandas as pd


def preprocess(storage_path: str):
    bookings = pd.read_csv(
        f"{storage_path}/raw_data.csv",
        dtype={
            "user_id": str,
            "item_id": str,
            "offer_subcategoryid": str,
            "offer_categoryId": str,
            "genres": str,
            "rayon": str,
            "type": str,
            "venue_id": str,
            "venue_name": str,
            "count": int,
        },
    )
    bookings.rename(columns={"count": "rating"}, inplace=True)
    bookings.to_csv(f"{storage_path}/clean_data.csv", index=False)
