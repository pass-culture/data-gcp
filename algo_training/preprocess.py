import pandas as pd

from utils import STORAGE_PATH, MODEL_NAME
from utils.v1.preprocess_tools import preprocess
from utils.v2.deep_reco.preprocess_tools import lighten_matrice


def main():
    if MODEL_NAME == "v1":
        preprocess(STORAGE_PATH)
    if MODEL_NAME == "v2_deep_reco":
        lighten_matrice(STORAGE_PATH)
    if MODEL_NAME == "v2_mf_reco":
        # Weigth interactions
        bookings = weighted_interactions(
            pd.read_csv(f"{storage_path}/raw_data_bookings.csv")
        )
        clicks = weighted_interactions(
            pd.read_csv(f"{storage_path}/raw_data_clicks.csv")
        )
        favorites = weighted_fav(pd.read_csv(f"{storage_path}/raw_data_favorites.csv"))
        grouped_df = group_data(bookings, clicks, favorites)


if __name__ == "__main__":
    main()
