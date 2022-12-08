import pandas as pd


def preprocess(raw_data_path: str):
    clean_data = pd.read_csv(
        raw_data_path,
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
    return clean_data
