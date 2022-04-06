import pandas as pd

from tools.utils import (
    GCP_PROJECT,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    DATA_GCS_BUCKET_NAME,
)
from tools.diversification_kpi import (
    calculate_diversification_per_feature,
    fuse_columns_into_format,
)


def get_data_diversification():
    query = f"""SELECT DISTINCT user_id, user_region_name, user_activity, 
    user_civility, user_deposit_creation_date, user_total_deposit_amount, actual_amount_spent
    FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data
    WHERE user_total_deposit_amount = 300 AND actual_amount_spent>=0 """
    data = pd.read_gbq(query)
    data["user_civility"] = data["user_civility"].replace(["M.", "Mme"], ["M", "F"])
    print("1")
    return data


def get_users_bookings(data):
    query = f"""SELECT user_id, offer.offer_id, roffer.offer_description,booking_creation_date, booking_amount,
    offer_category_id as category, bkg.offer_subcategoryId as subcategory, bkg.physical_goods, 
    bkg.digital_goods, bkg.event, offer.genres, offer.rayon, offer.type, offer.venue_id, offer.venue_name
    FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_booking_data bkg
    JOIN {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_offer_data as offer
    ON bkg.offer_id = offer.offer_id
    JOIN {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer as roffer
    ON bkg.offer_id = roffer.offer_id
    WHERE booking_is_cancelled<> True 
    AND bkg.user_id IN ( """
    for user in data["user_id"]:
        query = query + f"'{user}',"
    query = query[:-2] + """ ')"""
    users_bookings = pd.read_gbq(query)
    print("2")
    return users_bookings


def data_preparation():
    users_sample = get_data_diversification()
    bookings = get_users_bookings(users_sample)
    print("3")
    bookings_enriched = pd.merge(bookings, users_sample, on="user_id", validate="m:1")
    print("4")
    bookings_enriched["offer_description"] = (
        '"' + bookings_enriched["offer_description"] + '"'
    )
    print("23")
    bookings_enriched["format"] = bookings_enriched.apply(
        lambda x: fuse_columns_into_format(
            x["physical_goods"], x["digital_goods"], x["event"]
        ),
        axis=1,
    )
    print("5")
    return bookings_enriched


def get_rayon():
    data_rayon = pd.read_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/macron_rayon/correspondance_rayon_macro_rayon.csv",
        sep=",",
    )
    data_rayon = data_rayon.drop(columns=["Unnamed: 0"])
    print("6")
    return data_rayon


def get_users_qpi(users_sample):
    query = f"""SELECT * except(row_number)
    FROM ( select *, ROW_NUMBER() OVER (PARTITION BY user_id) as row_number
         FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_qpi_answers_v3

    )
    WHERE row_number=1
    AND user_id IN ("""
    for user in users_sample["user_id"]:
        query = query + f"'{user}',"
    query = query[:-2] + """ ')"""
    users_bookings = pd.read_gbq(query)
    print("7")
    return users_bookings


def diversification_kpi(df):
    df_dropped = df.drop(columns=["offer_description"])
    df_clean = df_dropped.rename(columns={"venue_id": "venue"})
    features = ["category", "subcategory", "format", "venue", "macro_rayon"]
    divers_per_feature = calculate_diversification_per_feature(df_clean, features)
    for feature in features:
        divers_col = {f"{feature}_diversification": divers_per_feature[feature]}
        df_clean = pd.merge(
            df_clean, pd.DataFrame(divers_col), left_index=True, right_index=True
        )

    divers_col = {f"qpi_diversification": divers_per_feature["qpi_diversification"]}
    df_clean = pd.merge(
        df_clean, pd.DataFrame(divers_col), left_index=True, right_index=True
    )
    # Calculate delta diversification
    features_qpi = features
    features_qpi.append("qpi")
    df_clean[f"delta_diversification"] = df_clean.apply(
        lambda x: sum(
            [float(x[f"{feature}_diversification"]) for feature in features_qpi]
        ),
        axis=1,
    )
    return df_clean


def main_get_data():
    bookings_enriched = data_preparation()
    df_cluster = get_rayon()
    data = pd.merge(bookings_enriched, df_cluster, on="rayon", how="left")
    qpi = get_users_qpi(data)
    qpi = qpi.drop(columns=["submitted_at"])
    data = pd.merge(data, qpi, on="user_id", how="left", validate="many_to_one")
    data = data.drop(
        columns=[
            "physical_goods",
            "digital_goods",
            "event",
            "genres",
            "rayon",
            "user_total_deposit_amount",
            "actual_amount_spent",
        ]
    )
    data = data.sort_values(by=["user_id", "booking_creation_date"])
    print("8")
    df = diversification_kpi(data)
    print("10")
    df.to_gbq(
        f"""{BIGQUERY_ANALYTICS_DATASET}.user_diversification""",
        project_id=f"{GCP_PROJECT}",
        if_exists="replace",
    )
    return
