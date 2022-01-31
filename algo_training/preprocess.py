import pandas as pd
import pandas_gbq as gbq
import numpy as np
from scipy.optimize import minimize, Bounds

from utils import STORAGE_PATH, MODEL_NAME
from tools.v1.preprocess_tools import preprocess
from tools.v2.deep_reco.preprocess_tools import lighten_matrice
from tools.v2.mf_reco.preprocess_tools import (
    get_weighted_interactions,
    group_data,
    data_prep,
    get_sparcity_filters,
    get_offers_and_users_to_keep,
    get_EAC_feedback,
)


def main():
    if MODEL_NAME == "v1":
        preprocess(STORAGE_PATH)
    if MODEL_NAME == "v2_deep_reco":
        lighten_matrice(STORAGE_PATH)
    if MODEL_NAME == "v2_mf_reco":
        # Weigth interactions
        bookings, clicks, favorites = get_weighted_interactions(STORAGE_PATH)
        grouped_df = group_data(bookings, clicks, favorites)
        minimal_user_strength, maximal_offer_strength = get_sparcity_filters(grouped_df)
        print(
            "SPARCITY CHECK (min,max):, ", minimal_user_strength, maximal_offer_strength
        )
        list_user_to_keep, list_offer_to_keep = get_offers_and_users_to_keep(
            grouped_df, minimal_user_strength, maximal_offer_strength
        )
        df_cleaned = grouped_df[grouped_df.offer_id.isin(list_offer_to_keep)]
        # Only filter on offers before get EAC feedback, the user filter could remove most of EAC users
        df_u15, df_u16, df_u17 = get_EAC_feedback(df_cleaned)
        df_u15.to_csv(f"{STORAGE_PATH}/clean_data_u15.csv")
        df_u16.to_csv(f"{STORAGE_PATH}/clean_data_u16.csv")
        df_u17.to_csv(f"{STORAGE_PATH}/clean_data_u17.csv")
        # Now we can filters the users to keep
        df_cleaned = df_cleaned[df_cleaned.user_id.isin(list_user_to_keep)]
        grouped_purchased = data_prep(df_cleaned)
        grouped_purchased.to_csv(f"{STORAGE_PATH}/clean_data.csv")


if __name__ == "__main__":
    main()
