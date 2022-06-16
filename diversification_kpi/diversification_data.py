import pandas as pd
import time
from multiprocessing import cpu_count, Pool


exitFlag = 0
BATCH_SIZE = 50000

from tools.utils import (
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
    DATA_GCS_BUCKET_NAME,
    TABLE_NAME,
)
from tools.diversification_kpi import (
    calculate_diversification_per_feature,
)


def count_data():
    query = f"""SELECT count(DISTINCT user_id) as nb
        FROM {GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data 
    """
    count = pd.read_gbq(query)
    return count.iloc[0]["nb"]


def get_data(batch, batch_size):
    query = f"""WITH batch_users AS (
                  SELECT DISTINCT user_id, user_region_name, user_activity, user_civility, user_deposit_creation_date, user_total_deposit_amount, actual_amount_spent, user_department_code
                  FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data`
                  ORDER BY user_id
                  LIMIT {batch_size} OFFSET {batch * batch_size}             
                ),
                
                bookings AS (
                  SELECT user_id, offer_id, booking_amount, booking_creation_date, booking_id, offer_subcategoryId, physical_goods, digital_goods, event, offer_category_id
                  FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_booking_data`
                ),
                
                offer AS (
                  SELECT offer_id, genres, rayon, type, venue_id, venue_name
                  FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_offer_data`
                ),
                
                
                qpi_answers AS (
                  SELECT * except(row_number)
                  FROM ( select *, ROW_NUMBER() OVER (PARTITION BY user_id) as row_number
                    FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.enriched_qpi_answers_v3` ) 
                  WHERE row_number=1)
                
                
                SELECT batch_users.user_id, bookings.booking_creation_date, bookings.booking_id, user_region_name, user_activity,
                  REPLACE(REPLACE(user_civility, 'M.', 'M'),'Mme','F') as user_civility, 
                  COALESCE(
                    IF(bookings.physical_goods = True, 'physical', null),
                    IF(bookings.digital_goods = True, 'digital', null),
                    IF(bookings.event = True, 'event', null)
                  ) as format,
                  user_deposit_creation_date, user_total_deposit_amount, actual_amount_spent, offer.offer_id, booking_amount, user_department_code,
                  bookings.offer_category_id as category, bookings.offer_subcategoryId as subcategory, offer.genres, offer.rayon, offer.type, offer.venue_id, offer.venue_name,
                  qpi_answers.*
                FROM batch_users
                INNER JOIN bookings
                ON batch_users.user_id = bookings.user_id
                INNER JOIN offer
                ON bookings.offer_id = offer.offer_id
                LEFT JOIN qpi_answers
                ON batch_users.user_id = qpi_answers.user_id
                ORDER BY batch_users.user_id"""
    data = pd.read_gbq(query)
    return data


def get_rayon():
    data_rayon = pd.read_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/macro_rayon/correspondance_rayon_macro_rayon.csv",
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
    divers_col = {f"delta_diversification": divers_per_feature["delta_diversification"]}
    df_clean = pd.merge(
        df_clean, pd.DataFrame(divers_col), left_index=True, right_index=True
    )
    return df_clean


def process_diversification(batch_number):
    bookings = get_data(batch_number, BATCH_SIZE)

    bookings_enriched = pd.merge(bookings, macro_rayons, on="rayon", how="left")
    bookings_sorted = bookings_enriched.sort_values(
        by=["user_id", "booking_creation_date"], ignore_index=True
    )
    df = diversification_kpi(bookings_sorted)
    df = df[
        [
            "user_id",
            "offer_id",
            "booking_id",
            "booking_creation_date",
            "category",
            "subcategory",
            "type",
            "venue",
            "venue_name",
            "user_region_name",
            "user_department_code",
            "user_activity",
            "user_civility",
            "booking_amount",
            "user_deposit_creation_date",
            "format",
            "macro_rayon",
            "category_diversification",
            "subcategory_diversification",
            "format_diversification",
            "venue_diversification",
            "macro_rayon_diversification",
            "qpi_diversification",
            "delta_diversification",
        ]
    ]
    df.to_gbq(
        f"""{BIGQUERY_ANALYTICS_DATASET}.{TABLE_NAME}""",
        project_id=f"{GCP_PROJECT}",
        if_exists="append",
        table_schema=[
            {"name": "user_id", "type": "STRING"},
            {"name": "offer_id", "type": "STRING"},
            {"name": "booking_id", "type": "STRING"},
            {"name": "booking_creation_date", "type": "TIMESTAMP"},
            {"name": "category", "type": "STRING"},
            {"name": "subcategory", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "venue", "type": "STRING"},
            {"name": "venue_name", "type": "STRING"},
            {"name": "user_region_name", "type": "STRING"},
            {"name": "user_department_code", "type": "STRING"},
            {"name": "user_activity", "type": "STRING"},
            {"name": "user_civility", "type": "STRING"},
            {"name": "booking_amount", "type": "FLOAT"},
            {"name": "user_deposit_creation_date", "type": "TIMESTAMP"},
            {"name": "format", "type": "STRING"},
            {"name": "macro_rayon", "type": "STRING"},
            {"name": "category_diversification", "type": "FLOAT"},
            {"name": "subcategory_diversification", "type": "FLOAT"},
            {"name": "format_diversification", "type": "FLOAT"},
            {"name": "venue_diversification", "type": "FLOAT"},
            {"name": "macro_rayon_diversification", "type": "FLOAT"},
            {"name": "qpi_diversification", "type": "INTEGER"},
            {"name": "delta_diversification", "type": "FLOAT"},
        ],
    )


if __name__ == "__main__":
    count = count_data()
    macro_rayons = get_rayon()
    max_batch = int(
        -1 * (-count // BATCH_SIZE)
    )  # roof division to get number of batches
    max_process = cpu_count()

    print(
        f"Starting process of {count} users by batch of {BATCH_SIZE} users.\nHence a total of {max_batch} batch(es)"
    )

    with Pool(max_process) as p:
        p.map(process_diversification, range(max_batch))

    time.sleep(60)
    print(f"End of calculation.")
