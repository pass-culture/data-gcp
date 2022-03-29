import pandas as pd

from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    DATA_GCS_BUCKET_NAME,
)


def get_data_diversification():
    query = f"""SELECT DISTINCT user_id, user_region_name, user_activity, 
    user_civility, user_deposit_creation_date, user_total_deposit_amount, actual_amount_spent
    FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data
    WHERE user_total_deposit_amount = 300 AND actual_amount_spent>295 """
    data = pd.read_gbq(query)
    data["user_civility"] = data["user_civility"].replace(["M.", "Mme"], ["M", "F"])
    return data


def fuse_columns_into_format(is_physical_good, is_digital_good, is_event):
    b_format = ""
    if is_physical_good == True:
        b_format = "physical"
    elif is_digital_good == True:
        b_format = "digital"
    elif is_event == True:
        b_format = "event"
    return b_format


def get_users_bookings(data):
    query = """SELECT user_id, offer.offer_id, roffer.offer_description,booking_creation_date, booking_amount,
    offer_category_id as category, bkg.offer_subcategoryId as subcategory, bkg.physical_goods, 
    bkg.digital_goods, bkg.event, offer.genres, offer.rayon, offer.type, offer.venue_id, offer.venue_name
    FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_booking_data bkg
    JOIN {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_offer_data as offer
    ON bkg.offer_id = offer.offer_id
    JOIN {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer as roffer
    ON bkg.offer_id = roffer.offer_id
    WHERE booking_is_cancelled<> True 
    AND bkg.user_id IN ("""
    for user in data["user_id"]:
        query = query + f"'{user}', "
    query = query[:-2] + ")"
    users_bookings = pd.read_gbq(query)
    return users_bookings
