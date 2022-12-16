import pandas as pd


def preprocess(raw_data: pd.DataFrame):
    clean_data = raw_data.astype(
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
