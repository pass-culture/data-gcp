import pandas as pd
from datetime import datetime, timedelta

from utils import STORAGE_PATH, BOOKING_DAY_NUMBER, MODEL_NAME
from tools.v1.data_collect_queries import get_bookings
from tools.v2.mf_reco.data_collect_queries import get_firebase_event, get_bookings_v2_mf
from tools.v2.deep_reco.data_collect_queries import get_clics


def main():
    start_date = (datetime.now() - timedelta(days=BOOKING_DAY_NUMBER)).strftime(
        "%Y-%m-%d"
    )
    end_date = datetime.now().strftime("%Y-%m-%d")
    test_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if MODEL_NAME == "v1":
        bookings = get_bookings(start_date=start_date, end_date=end_date)
        bookings.to_csv(f"{STORAGE_PATH}/raw_data.csv")
    if MODEL_NAME == "v2_deep_reco":
        clics = get_clics(start_date, end_date, test_date)
        clics.to_csv(f"{STORAGE_PATH}/raw_data.csv")
    if MODEL_NAME == "v2_mf_reco":
        clicks = get_firebase_event(
            start_date=start_date, end_date=end_date, event_type="ConsultOffer"
        )
        clicks.to_csv(f"{STORAGE_PATH}/raw_data_clicks.csv")
        favorites = get_firebase_event(
            start_date=start_date,
            end_date=end_date,
            event_type="HasAddedOfferToFavorites",
        )
        favorites.to_csv(f"{STORAGE_PATH}/raw_data_favorites.csv")
        bookings = get_bookings_v2_mf(start_date=start_date, end_date=end_date)
        bookings.to_csv(f"{STORAGE_PATH}/raw_data_bookings.csv")


if __name__ == "__main__":
    main()
