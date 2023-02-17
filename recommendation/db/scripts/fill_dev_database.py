"""
This script allows to populate the cloudsql_recommendation_dev postgres database with test data. Its main goal is to
provide the data necessary to the integration tests of the recommendation API (in dev).

Before to run this Python script, you need to start a proxy on your local computer by running the following docker
command:

docker run -d \
  -v /path/to/gcp/credentials.json:/config \
  -p 127.0.0.1:5432:5432 \
  gcr.io/cloudsql-docker/gce-proxy:1.19.1 /cloud_sql_proxy \
  -instances=passculture-data-ehp:europe-west1:cloudsql-recommendation-dev=tcp:0.0.0.0:5432 -credential_file=/config

"""
import os
from datetime import datetime, timedelta

import pandas as pd
from loguru import logger
import sqlalchemy

RECOMMENDATION_CLOUDSQL_PASSWORD_DEV = os.environ.get(
    "RECOMMENDATION_CLOUDSQL_PASSWORD_DEV"
)
SQL_CONNECTION_NAME = "passculture-data-ehp:europe-west1:cloudsql-recommendation-dev"
query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)
cloud_sql_engine = sqlalchemy.create_engine(
    f"postgresql://cloudsql-recommendation-dev:{RECOMMENDATION_CLOUDSQL_PASSWORD_DEV}@localhost:5432/cloudsql-recommendation-dev"
)

today = datetime.now()
last_week = datetime.now() - timedelta(days=7)
unique_id_list = [str(unique_id) for unique_id in list(range(20))]


offer = pd.DataFrame(
    {
        "offer_id_at_providers": [None] * 20,
        "offer_modified_at_last_provider_date": [last_week] * 20,
        "offer_id": unique_id_list,
        "offer_creation_date": [last_week] * 20,
        "offer_product_id": unique_id_list,
        "venue_id": unique_id_list,
        "offer_last_provider_id": [None] * 20,
        "booking_email": ["test@email.com"] * 20,
        "offer_is_active": [True] * 20,
        "offer_name": ["test"] * 20,
        "offer_description": ["test"] * 20,
        "offer_conditions": [None] * 20,
        "offer_age_min": [None] * 20,
        "offer_age_max": [None] * 20,
        "offer_url": ["test_url"] * 20,
        "offer_duration_minutes": [60] * 20,
        "offer_is_national": [True] * 20,
        "offer_extra_data": [None] * 20,
        "offer_is_duo": [False] * 20,
        "offer_withdrawal_details": [None] * 20,
        "offer_validation": ["APPROVED"] * 20,
    }
)
offer_dtype = {
    "offer_id": sqlalchemy.types.VARCHAR(length=255),
    "offer_product_id": sqlalchemy.types.VARCHAR(length=255),
    "offer_name": sqlalchemy.types.VARCHAR(length=255),
    "offer_url": sqlalchemy.types.VARCHAR(length=255),
    "venue_id": sqlalchemy.types.VARCHAR(length=255),
}

booking = pd.DataFrame(
    {
        "booking_id": unique_id_list,
        "booking_creation_date": [today] * 20,
        "stock_id": unique_id_list,
        "booking_quantity": [10] * 20,
        "user_id": unique_id_list,
        "booking_amount": [38.00] * 20,
        "booking_is_cancelled": [False] * 20,
        "booking_is_used": [True] * 20,
        "booking_used_date": [None] * 20,
        "booking_cancellation_date": [None] * 20,
    }
)
booking_dtype = {
    "user_id": sqlalchemy.types.VARCHAR(length=255),
    "stock_id": sqlalchemy.types.VARCHAR(length=255),
}

venue = pd.DataFrame(
    {
        "venue_thumb_count": [10] * 20,
        "venue_id_at_providers": [None] * 20,
        "venue_modified_at_last_provider": [last_week] * 20,
        "venue_address": [None] * 20,
        "venue_postal_code": [None] * 20,
        "venue_city": [None] * 20,
        "venue_id": unique_id_list,
        "venue_name": ["Offrenumérique"] * 20,
        "venue_siret": [None] * 20,
        "venue_department_code": [None] * 20,
        "venue_latitude": [None] * 20,
        "venue_longitude": [None] * 20,
        "venue_managing_offerer_id": unique_id_list,
        "venue_booking_email": ["test@email"] * 20,
        "venue_last_provider_id": [None] * 20,
        "venue_is_virtual": [True] * 20,
        "venue_comment": [None] * 20,
        "venue_public_name": [None] * 20,
        "venue_type_id": [727] * 20,
        "venue_label_id": [None] * 20,
        "venue_creation_date": [last_week] * 20,
        "venue_validation_token": [None] * 20,
    }
)
venue_dtype = {"venue_id": sqlalchemy.types.VARCHAR(length=255)}

offerer = pd.DataFrame(
    {
        "offerer_is_active": [True] * 20,
        "offerer_thumb_count": [10] * 20,
        "offerer_id_at_providers": unique_id_list,
        "offerer_modified_at_last_provider_date": [last_week] * 20,
        "offerer_address": ["BOULEVARDDESJARDINIERS"] * 20,
        "offerer_postal_code": ["06200"] * 20,
        "offerer_city": ["NICE"] * 20,
        "offerer_id": unique_id_list,
        "offerer_creation_date": [last_week] * 20,
        "offerer_name": ["MUSEENATIONALDUSPORT"] * 20,
        "offerer_siren": [130002645] * 20,
        "offerer_last_provider_id": [None] * 20,
    }
)
offerer_dtype = {}

mediation = pd.DataFrame(
    {
        "thumbCount": [10] * 20,
        "idAtProviders": [None] * 20,
        "dateModifiedAtLastProvider": [last_week] * 20,
        "id": [4876] * 20,
        "dateCreated": [last_week] * 20,
        "authorId": [3814] * 20,
        "lastProviderId": [3814] * 20,
        "offerId": unique_id_list,
        "credit": ["undefined"] * 20,
        "isActive": [True] * 20,
    }
)
mediation_dtype = {}

stock = pd.DataFrame(
    {
        "stock_id_at_providers": [None] * 20,
        "stock_modified_at_last_provider_date": [last_week] * 20,
        "stock_id": unique_id_list,
        "stock_modified_date": [last_week] * 20,
        "stock_price": [0] * 20,
        "stock_quantity": [200] * 20,
        "stock_booking_limit_date": [None] * 20,
        "stock_last_provider_id": unique_id_list,
        "offer_id": unique_id_list,
        "stock_is_soft_deleted": [False] * 20,
        "stock_beginning_date": [None] * 20,
        "stock_creation_date": [last_week] * 20,
    }
)
stock_dtype = {
    "stock_beginning_date": sqlalchemy.types.DateTime(True),
    "stock_booking_limit_date": sqlalchemy.types.DateTime(True),
    "offer_id": sqlalchemy.types.VARCHAR(length=255),
}

TABLES = {
    "offer": {"pandas_dataframe": offer, "dtype": offer_dtype},
    "booking": {"pandas_dataframe": booking, "dtype": booking_dtype},
    "venue": {"pandas_dataframe": venue, "dtype": venue_dtype},
    "offerer": {"pandas_dataframe": offerer, "dtype": offerer_dtype},
    "mediation": {"pandas_dataframe": mediation, "dtype": mediation_dtype},
    "stock": {"pandas_dataframe": stock, "dtype": stock_dtype},
}

for table in TABLES:
    TABLES[table]["pandas_dataframe"].to_sql(
        table,
        con=cloud_sql_engine,
        if_exists="replace",
        index=False,
        dtype=TABLES[table]["dtype"],
    )
    logger.info(f"{table} table has been successfully replaced!")


connection = cloud_sql_engine.connect().execution_options(autocommit=True)

for materialized_view in [
    "recommendable_offers",
    "non_recommendable_offers",
    "number_of_bookings_per_user",
]:
    connection.execute(f"REFRESH MATERIALIZED VIEW {materialized_view};")
    logger.info(
        f"{materialized_view} materialized view has been successfully refreshed!"
    )

connection.close()
