import pandas as pd

from tools.utils import (
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
    DATA_GCS_BUCKET_NAME,
)
from tools.diversification_kpi import (
    calculate_diversification_per_feature,
    fuse_columns_into_format,
)


def get_data():
    query = f"""SELECT DISTINCT A.user_id, user_region_name, user_activity,
    user_civility, user_deposit_creation_date, user_total_deposit_amount, actual_amount_spent, offer.offer_id, booking_creation_date, booking_amount,
    offer_category_id as category, bkg.offer_subcategoryId as subcategory, bkg.physical_goods,
    bkg.digital_goods, bkg.event, offer.genres, offer.rayon, offer.type, offer.venue_id, offer.venue_name,C.*
    FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data A
    LEFT join  {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_booking_data as bkg
    ON A.user_id = bkg.user_id
    LEFT JOIN {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_offer_data as offer
    ON bkg.offer_id = offer.offer_id
    LEFT JOIN (
        SELECT * except(row_number)
        FROM ( select *, ROW_NUMBER() OVER (PARTITION BY user_id) as row_number
            FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_qpi_answers_v3
        )
    WHERE row_number=1) AS C
    ON A.user_id = C.user_id
    WHERE user_total_deposit_amount = 300"""
    data = pd.read_gbq(query)
    data["user_civility"] = data["user_civility"].replace(["M.", "Mme"], ["M", "F"])
    data["format"] = data.apply(
        lambda x: fuse_columns_into_format(
            x["physical_goods"], x["digital_goods"], x["event"]
        ),
        axis=1,
    )
    return data


def get_rayon():
    data_rayon = pd.read_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/macron_rayon/correspondance_rayon_macro_rayon.csv",
        sep=",",
    )
    data_rayon = data_rayon.drop(columns=["Unnamed: 0"])
    return data_rayon


def diversification_kpi(df):
    df_clean = df.rename(columns={"venue_id": "venue"})
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


if __name__ == "__main__":
    df = get_data()
    df_cluster = get_rayon()
    data = pd.merge(df, df_cluster, on="rayon", how="left")
    data = data.drop(columns=["submitted_at"])
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
    df = diversification_kpi(data)
    df.to_gbq(
        f"""{BIGQUERY_ANALYTICS_DATASET}.user_diversification""",
        project_id=f"{GCP_PROJECT}",
        if_exists="replace",
    )
